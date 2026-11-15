"""Portable SQLAlchemy Core persistence for SANYAM FIRMS ingestion.

The repository supports SQLite for local development and PostgreSQL for
production. Database-specific SQL is isolated here; ingestion code depends only
on DatabaseRepository methods.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    and_,
    exists,
    func,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.engine import Engine, URL, make_url
from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

DEFAULT_DATABASE_URL = "sqlite:///data/sanyam_firms.sqlite3"
INSERT_BATCH_SIZE = 1000


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware datetime that round-trips as UTC on SQLite and PostgreSQL.

    PostgreSQL stores TIMESTAMP WITH TIME ZONE. SQLite has no native timezone-
    aware timestamp type, so values are stored in its DATETIME representation;
    naive values read back from SQLite are explicitly interpreted as UTC.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect):  # type: ignore[no-untyped-def]
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value: datetime | None, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("UTCDateTime requires a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: datetime | None, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


metadata = MetaData()

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    Column("run_id", Integer, Identity(), primary_key=True),
    Column("dataset_id", String(64), nullable=False),
    Column("requested_start", String(10), nullable=False),
    Column("requested_end", String(10), nullable=False),
    Column("bbox", String(128), nullable=False),
    Column("started_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime(), nullable=True),
    Column("status", String(32), nullable=False),
    Column("fetched_rows", Integer, nullable=False, server_default=text("0")),
    Column("inserted_rows", Integer, nullable=False, server_default=text("0")),
    Column("duplicate_rows", Integer, nullable=False, server_default=text("0")),
    Column("rejected_rows", Integer, nullable=False, server_default=text("0")),
    # Retained for compatibility with existing run records. New code also
    # writes the normalized reference to raw_assets (SKH §6.10).
    Column("source_csv_path", Text, nullable=True),
    Column("source_sha256", String(64), nullable=True),
    Column("error_message", Text, nullable=True),
)

raw_assets = Table(
    "raw_assets",
    metadata,
    Column("raw_asset_id", Integer, Identity(), primary_key=True),
    Column("ingestion_run_id", Integer, ForeignKey("ingestion_runs.run_id", ondelete="CASCADE"), nullable=False),
    Column("asset_role", String(48), nullable=False, server_default=text("'FIRMS_SOURCE_SNAPSHOT'")),
    Column("storage_uri", Text, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("media_type", String(96), nullable=False, server_default=text("'text/csv'")),
    Column("byte_size", BigInteger, nullable=True),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("ingestion_run_id", "sha256", name="uq_raw_assets_run_sha256"),
)
idx_raw_assets_sha256 = Index("idx_raw_assets_sha256", raw_assets.c.sha256)

observations = Table(
    "observations",
    metadata,
    Column("observation_id", Integer, Identity(), primary_key=True),
    Column("source_row_hash", String(64), nullable=False, unique=True),
    Column("dataset_id", String(64), nullable=False),
    Column("processing_type", String(8), nullable=False),
    Column("sensor", String(16), nullable=False),
    Column("collection", String(16), nullable=True),
    Column("acquired_at", UTCDateTime(), nullable=False),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("satellite", String(32), nullable=True),
    Column("instrument", String(32), nullable=True),
    Column("confidence_raw", String(64), nullable=True),
    Column("frp_mw", Float, nullable=True),
    Column("brightness_k", Float, nullable=True),
    Column("bright_ti4_k", Float, nullable=True),
    Column("bright_ti5_k", Float, nullable=True),
    Column("scan", Float, nullable=True),
    Column("track", Float, nullable=True),
    Column("daynight", String(8), nullable=True),
    Column("source_version", String(64), nullable=True),
    Column("footprint_source", String(64), nullable=False,
          server_default=text("'FIRMS_POINT_SCAN_TRACK'")),
    Column("opportunity_state", String(32), nullable=False,
          server_default=text("'UNKNOWN'")),
    Column("raw_json", Text, nullable=False),
    Column("first_ingested_at", UTCDateTime(), nullable=False),
    Column("ingestion_run_id", Integer, ForeignKey("ingestion_runs.run_id"), nullable=False),
    CheckConstraint("processing_type IN ('SP', 'NRT')", name="ck_observations_processing_type"),
    CheckConstraint("sensor IN ('MODIS', 'VIIRS')", name="ck_observations_sensor"),
)
idx_observations_acquired_at = Index("idx_observations_acquired_at", observations.c.acquired_at)
idx_observations_dataset_time = Index("idx_observations_dataset_time", observations.c.dataset_id, observations.c.acquired_at)
idx_observations_sensor_collection = Index("idx_observations_sensor_collection", observations.c.sensor, observations.c.collection)
idx_observations_lat_lon = Index("idx_observations_lat_lon", observations.c.latitude, observations.c.longitude)
idx_runs_dataset_status = Index("idx_runs_dataset_status", ingestion_runs.c.dataset_id, ingestion_runs.c.status)

# SKH Master Documentation §6.10 — processing and analysis result entities.
processing_runs = Table(
    "processing_runs",
    metadata,
    Column("processing_run_id", Integer, Identity(), primary_key=True),
    Column("module_name", String(24), nullable=False),
    Column("method_version", String(64), nullable=False),
    Column("configuration_json", Text, nullable=False),
    Column("started_at", UTCDateTime(), nullable=False),
    Column("completed_at", UTCDateTime(), nullable=True),
    Column("input_count", Integer, nullable=False, server_default=text("0")),
    Column("output_count", Integer, nullable=False, server_default=text("0")),
    Column("status", String(24), nullable=False),
    Column("error_message", Text, nullable=True),
    CheckConstraint(
        "module_name IN ('QC', 'OPPORTUNITY', 'MATCHING', 'MEASUREMENT', 'AGGREGATION')",
        name="ck_processing_runs_module",
    ),
)
idx_processing_runs_module_method = Index(
    "idx_processing_runs_module_method",
    processing_runs.c.module_name,
    processing_runs.c.method_version,
    processing_runs.c.status,
)

qc_results = Table(
    "qc_results",
    metadata,
    Column("qc_result_id", Integer, Identity(), primary_key=True),
    Column("observation_id", Integer, ForeignKey("observations.observation_id", ondelete="RESTRICT"), nullable=False),
    Column("processing_run_id", Integer, ForeignKey("processing_runs.processing_run_id", ondelete="RESTRICT"), nullable=False),
    Column("quality_status", String(32), nullable=False),
    Column("suitability_for_matching", Boolean, nullable=False),
    Column("suitability_for_frp", Boolean, nullable=False),
    Column("warnings_json", Text, nullable=False),
    Column("source_confidence_category", String(16), nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("processing_run_id", "observation_id", name="uq_qc_run_observation"),
    CheckConstraint(
        "quality_status IN ('VALID', 'VALID_WITH_WARNINGS', 'INVALID', 'INCOMPLETE', 'QUARANTINED')",
        name="ck_qc_quality_status",
    ),
    CheckConstraint(
        "source_confidence_category IN ('LOW', 'NOMINAL', 'HIGH', 'UNKNOWN')",
        name="ck_qc_confidence_category",
    ),
)
idx_qc_results_observation = Index("idx_qc_results_observation", qc_results.c.observation_id)
idx_qc_results_run = Index("idx_qc_results_run", qc_results.c.processing_run_id)

observation_opportunities = Table(
    "observation_opportunities",
    metadata,
    Column("opportunity_id", Integer, Identity(), primary_key=True),
    Column("processing_run_id", Integer, ForeignKey("processing_runs.processing_run_id", ondelete="RESTRICT"), nullable=False),
    # Nullable for area/time summaries that have no matching detected observation.
    Column("observation_id", Integer, ForeignKey("observations.observation_id", ondelete="RESTRICT"), nullable=True),
    Column("sensor", String(16), nullable=False),
    Column("platform", String(32), nullable=False),
    Column("spatial_unit_id", String(128), nullable=False),
    Column("time_window_start", UTCDateTime(), nullable=False),
    Column("time_window_end", UTCDateTime(), nullable=False),
    Column("opportunity_state", String(32), nullable=False),
    Column("evidence_source", String(48), nullable=False),
    Column("notes", Text, nullable=True),
    Column("created_at", UTCDateTime(), nullable=False),
    CheckConstraint("sensor IN ('MODIS', 'VIIRS')", name="ck_opportunity_sensor"),
    CheckConstraint(
        "opportunity_state IN ('DETECTED', 'VALID_NO_DETECTION', 'CLOUD_OBSCURED', "
        "'INVALID_QUALITY', 'NOT_COVERED', 'UNKNOWN')",
        name="ck_opportunity_state",
    ),
    CheckConstraint(
        "evidence_source IN ('FIRMS_DETECTION_ONLY', 'LEVEL2_SWATH_MASK')",
        name="ck_opportunity_evidence_source",
    ),
    CheckConstraint("time_window_end > time_window_start", name="ck_opportunity_time_window"),
)
idx_opportunities_lookup = Index(
    "idx_opportunities_lookup",
    observation_opportunities.c.sensor,
    observation_opportunities.c.spatial_unit_id,
    observation_opportunities.c.time_window_start,
    observation_opportunities.c.time_window_end,
)

# SKH §§6.10 and 7.2–7.7 — Module 6 scientific outputs.
candidate_associations = Table(
    "candidate_associations",
    metadata,
    Column("candidate_id", Integer, Identity(), primary_key=True),
    Column("processing_run_id", Integer, ForeignKey("processing_runs.processing_run_id", ondelete="RESTRICT"), nullable=False),
    # One side is NULL for an explicit UNMATCHED node record.
    Column("modis_observation_id", Integer, ForeignKey("observations.observation_id", ondelete="RESTRICT"), nullable=True),
    Column("viirs_observation_id", Integer, ForeignKey("observations.observation_id", ondelete="RESTRICT"), nullable=True),
    Column("matching_method", String(32), nullable=False),
    Column("time_difference_minutes", Float, nullable=True),
    Column("centroid_distance_km", Float, nullable=True),
    Column("intersection_area_km2", Float, nullable=True),
    Column("iou", Float, nullable=True),
    Column("smaller_footprint_overlap", Float, nullable=True),
    Column("boundary_distance_km", Float, nullable=True),
    Column("geometry_source", String(48), nullable=True),
    Column("relationship_status", String(32), nullable=False),
    Column("modis_degree", Integer, nullable=True),
    Column("viirs_degree", Integer, nullable=True),
    Column("component_id", String(64), nullable=True),
    Column("config_json", Text, nullable=False),
    UniqueConstraint(
        "processing_run_id", "matching_method", "modis_observation_id", "viirs_observation_id",
        name="uq_candidate_run_method_pair",
    ),
    CheckConstraint(
        "matching_method IN ('BASELINE_HAVERSINE', 'FOOTPRINT_AWARE')",
        name="ck_candidate_matching_method",
    ),
    CheckConstraint(
        "relationship_status IN ('SINGLE_CANDIDATE', 'MULTIPLE_CANDIDATES', 'AMBIGUOUS_GROUP', 'UNMATCHED', 'REJECTED')",
        name="ck_candidate_relationship_status",
    ),
    CheckConstraint(
        "modis_observation_id IS NOT NULL OR viirs_observation_id IS NOT NULL",
        name="ck_candidate_has_observation",
    ),
)
idx_candidate_run = Index("idx_candidate_run", candidate_associations.c.processing_run_id)
idx_candidate_modis = Index("idx_candidate_modis", candidate_associations.c.modis_observation_id)
idx_candidate_viirs = Index("idx_candidate_viirs", candidate_associations.c.viirs_observation_id)

measurement_results = Table(
    "measurement_results",
    metadata,
    Column("measurement_result_id", Integer, Identity(), primary_key=True),
    Column("processing_run_id", Integer, ForeignKey("processing_runs.processing_run_id", ondelete="RESTRICT"), nullable=False),
    Column("observation_id", Integer, ForeignKey("observations.observation_id", ondelete="RESTRICT"), nullable=False),
    Column("frp_raw_mw", Float, nullable=True),
    Column("frp_adjusted_mw", Float, nullable=True),
    Column("adjustment_method", String(96), nullable=True),
    Column("method_version", String(64), nullable=False),
    Column("detection_limit_mw", Float, nullable=True),
    Column("pixel_area_km2", Float, nullable=True),
    Column("evidence_category", String(40), nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("processing_run_id", "observation_id", name="uq_measurement_run_observation"),
    CheckConstraint(
        "evidence_category IN ('DIRECT_SOURCE_OBSERVATION', 'MODEL_ADJUSTED')",
        name="ck_measurement_evidence_category",
    ),
)
idx_measurement_run = Index("idx_measurement_run", measurement_results.c.processing_run_id)
idx_measurement_observation = Index("idx_measurement_observation", measurement_results.c.observation_id)

aggregation_results = Table(
    "aggregation_results",
    metadata,
    Column("agg_id", Integer, Identity(), primary_key=True),
    Column("processing_run_id", Integer, ForeignKey("processing_runs.processing_run_id", ondelete="RESTRICT"), nullable=False),
    Column("grid_cell_id", String(128), nullable=False),
    Column("grid_resolution_km", Float, nullable=False),
    # RFC 7946 export geometry is WGS84 longitude/latitude WKT. Area calculations
    # are performed in the equal-area CRS recorded in processing_runs.configuration_json.
    Column("cell_geometry_wkt", Text, nullable=False),
    Column("period_start", UTCDateTime(), nullable=False),
    Column("period_end", UTCDateTime(), nullable=False),
    Column("modis_count", Float, nullable=False, server_default=text("0")),
    Column("viirs_count", Float, nullable=False, server_default=text("0")),
    Column("matched_group_count", Integer, nullable=True),
    Column("modis_frp_sum_mw", Float, nullable=True),
    Column("viirs_frp_sum_mw", Float, nullable=True),
    Column("valid_area_fraction", Float, nullable=True),
    Column("opportunity_summary", Text, nullable=False),
    Column("activity_status", String(32), nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    UniqueConstraint("processing_run_id", "grid_cell_id", "period_start", "period_end", name="uq_aggregation_run_cell_period"),
    CheckConstraint("grid_resolution_km > 0", name="ck_aggregation_grid_resolution"),
    CheckConstraint("period_end > period_start", name="ck_aggregation_period"),
)
idx_aggregation_run = Index("idx_aggregation_run", aggregation_results.c.processing_run_id)
idx_aggregation_grid_period = Index(
    "idx_aggregation_grid_period", aggregation_results.c.grid_cell_id,
    aggregation_results.c.period_start, aggregation_results.c.period_end,
)


def resolve_database_url(value: str | Path | None = None) -> str:
    """Resolve an explicit URL/path, DATABASE_URL, or the SQLite default."""
    candidate = value if value is not None else os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    if isinstance(candidate, Path):
        path = candidate.expanduser()
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        return URL.create("sqlite", database=str(path)).render_as_string(hide_password=False)

    candidate = str(candidate).strip()
    if not candidate:
        candidate = DEFAULT_DATABASE_URL
    if "://" not in candidate:
        path = Path(candidate).expanduser()
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        return URL.create("sqlite", database=str(path)).render_as_string(hide_password=False)

    parsed = make_url(candidate)
    if parsed.get_backend_name() == "sqlite":
        database = parsed.database
        if database and database not in {":memory:"} and not database.startswith("file:"):
            db_path = Path(database).expanduser()
            if db_path.parent != Path("."):
                db_path.parent.mkdir(parents=True, exist_ok=True)
    if parsed.get_backend_name() == "postgresql" and parsed.drivername == "postgresql":
        parsed = parsed.set(drivername="postgresql+psycopg")
    return parsed.render_as_string(hide_password=False)


def _sqlite_connection_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
    """SQLite-only connection settings. Never registered for PostgreSQL."""
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
    finally:
        cursor.close()


def _column_names(engine: Engine, table_name: str) -> set[str]:
    return {column["name"] for column in inspect(engine).get_columns(table_name)}


def _migrate_additive_observation_columns(engine: Engine) -> None:
    """Add fields to pre-existing SQLite/PostgreSQL observations tables safely."""
    if "observations" not in inspect(engine).get_table_names():
        return
    existing = _column_names(engine, "observations")
    additions = {
        "sensor": "VARCHAR(16)",
        "collection": "VARCHAR(16)",
        "footprint_source": "VARCHAR(64) NOT NULL DEFAULT 'FIRMS_POINT_SCAN_TRACK'",
        "opportunity_state": "VARCHAR(32) NOT NULL DEFAULT 'UNKNOWN'",
    }
    with engine.begin() as connection:
        for name, ddl_type in additions.items():
            if name not in existing:
                connection.execute(text(f'ALTER TABLE observations ADD COLUMN "{name}" {ddl_type}'))

        # Backfill only values that are deterministically derivable from the
        # FIRMS dataset ID. Collection remains NULL if source version metadata
        # cannot establish it; we do not invent a collection value.
        connection.execute(text("""
            UPDATE observations
               SET sensor = CASE
                   WHEN UPPER(dataset_id) LIKE 'MODIS%' THEN 'MODIS'
                   WHEN UPPER(dataset_id) LIKE 'VIIRS%' THEN 'VIIRS'
                   ELSE sensor
               END
             WHERE sensor IS NULL
        """))
        # Backfill collection from the retained FIRMS product version where
        # the prefix is recognizable. Unknown versions remain NULL.
        legacy_versions = connection.execute(
            select(observations.c.observation_id, observations.c.source_version)
            .where(
                observations.c.collection.is_(None),
                observations.c.source_version.is_not(None),
            )
        ).all()
        for observation_id, source_version in legacy_versions:
            match = re.match(r"^\s*(\d+)(?:\.(\d+))?", str(source_version or ""))
            if not match:
                continue
            major, minor = match.groups()
            collection = major if minor is None or set(minor) <= {"0"} else f"{major}.{minor}"
            connection.execute(
                update(observations)
                .where(observations.c.observation_id == observation_id)
                .values(collection=collection)
            )


def _migrate_postgresql_timestamp_columns(engine: Engine) -> None:
    """Convert legacy text/naive timestamp columns to PostgreSQL TIMESTAMPTZ."""
    if engine.dialect.name != "postgresql":
        return
    targets = {
        "ingestion_runs": ("started_at", "completed_at"),
        "observations": ("acquired_at", "first_ingested_at"),
    }
    inspector = inspect(engine)
    with engine.begin() as connection:
        for table_name, columns in targets.items():
            if table_name not in inspector.get_table_names():
                continue
            for column in inspector.get_columns(table_name):
                name = column["name"]
                if name not in columns:
                    continue
                col_type = column["type"]
                if isinstance(col_type, DateTime) and getattr(col_type, "timezone", False):
                    continue
                # Legacy values written by this ingester are UTC ISO strings.
                # Preserve explicit Z/offset suffixes; interpret naive legacy
                # timestamps as UTC rather than using the server session TZ.
                connection.execute(text(
                    f'ALTER TABLE "{table_name}" ALTER COLUMN "{name}" '
                    f'TYPE TIMESTAMP WITH TIME ZONE USING '
                    f'CASE WHEN "{name}" IS NULL THEN NULL '
                    f"WHEN \"{name}\"::text ~ '(Z|z|[+-][0-9]{{2}}(:[0-9]{{2}}|[0-9]{{2}})?)$' "
                    f'THEN "{name}"::text::timestamptz '
                    f"ELSE (\"{name}\"::text::timestamp without time zone AT TIME ZONE 'UTC') END"
                ))


def _backfill_raw_assets(engine: Engine) -> None:
    """Represent legacy run-level CSV metadata as RawAsset records (SKH §6.10)."""
    with engine.begin() as connection:
        rows = connection.execute(
            select(
                ingestion_runs.c.run_id,
                ingestion_runs.c.source_csv_path,
                ingestion_runs.c.source_sha256,
            ).where(
                ingestion_runs.c.source_csv_path.is_not(None),
                ingestion_runs.c.source_sha256.is_not(None),
            )
        ).mappings().all()
        existing = {
            (run_id, sha256)
            for run_id, sha256 in connection.execute(
                select(raw_assets.c.ingestion_run_id, raw_assets.c.sha256)
            ).all()
        }
        now = datetime.now(timezone.utc)
        payload = []
        for row in rows:
            key = (row["run_id"], row["source_sha256"])
            if key in existing:
                continue
            path = Path(row["source_csv_path"])
            size = path.stat().st_size if path.exists() else None
            payload.append({
                "ingestion_run_id": row["run_id"],
                "asset_role": "FIRMS_SOURCE_SNAPSHOT",
                "storage_uri": str(path),
                "sha256": row["source_sha256"],
                "media_type": "text/csv",
                "byte_size": size,
                "created_at": now,
            })
        if payload:
            connection.execute(raw_assets.insert(), payload)


def _migrate_processing_run_modules(engine: Engine) -> None:
    """Expand processing_runs module names while preserving existing rows/FKs."""
    if "processing_runs" not in inspect(engine).get_table_names():
        return
    allowed = "'QC', 'OPPORTUNITY', 'MATCHING', 'MEASUREMENT', 'AGGREGATION'"
    if engine.dialect.name == "postgresql":
        with engine.connect() as connection:
            definition = connection.execute(text("""
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = 'processing_runs'::regclass
                  AND conname = 'ck_processing_runs_module'
            """)).scalar_one_or_none()
        if definition and "MATCHING" in definition and "AGGREGATION" in definition:
            return
        with engine.begin() as connection:
            connection.execute(text(
                "ALTER TABLE processing_runs DROP CONSTRAINT IF EXISTS ck_processing_runs_module"
            ))
            connection.execute(text(
                f"ALTER TABLE processing_runs ADD CONSTRAINT ck_processing_runs_module "
                f"CHECK (module_name IN ({allowed}))"
            ))
        return
    if engine.dialect.name != "sqlite":
        return
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.commit()
        try:
            with connection.begin():
                schema = connection.exec_driver_sql(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='processing_runs'"
                ).scalar_one_or_none() or ""
                if "'MATCHING'" in schema and "'AGGREGATION'" in schema:
                    return
                connection.exec_driver_sql("""
                    CREATE TABLE processing_runs_new (
                        processing_run_id INTEGER NOT NULL PRIMARY KEY,
                        module_name VARCHAR(24) NOT NULL,
                        method_version VARCHAR(64) NOT NULL,
                        configuration_json TEXT NOT NULL,
                        started_at DATETIME NOT NULL,
                        completed_at DATETIME,
                        input_count INTEGER NOT NULL DEFAULT 0,
                        output_count INTEGER NOT NULL DEFAULT 0,
                        status VARCHAR(24) NOT NULL,
                        error_message TEXT,
                        CONSTRAINT ck_processing_runs_module
                        CHECK (module_name IN ('QC','OPPORTUNITY','MATCHING','MEASUREMENT','AGGREGATION'))
                    )
                """)
                connection.exec_driver_sql("""
                    INSERT INTO processing_runs_new (
                        processing_run_id, module_name, method_version, configuration_json,
                        started_at, completed_at, input_count, output_count, status, error_message
                    )
                    SELECT processing_run_id, module_name, method_version, configuration_json,
                           started_at, completed_at, input_count, output_count, status, error_message
                    FROM processing_runs
                """)
                connection.exec_driver_sql("DROP TABLE processing_runs")
                connection.exec_driver_sql("ALTER TABLE processing_runs_new RENAME TO processing_runs")
        finally:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.commit()


class DatabaseRepository:
    """Dialect-aware persistence repository for the FIRMS ingestion pipeline."""

    def __init__(self, engine: Engine):
        self.engine = engine

    @property
    def dialect_name(self) -> str:
        return self.engine.dialect.name

    @property
    def display_target(self) -> str:
        return self.engine.url.render_as_string(hide_password=True)

    def close(self) -> None:
        self.engine.dispose()

    def has_completed_chunk(
        self,
        dataset_id: str,
        start_date: date,
        end_date: date,
        bbox_value: str,
    ) -> bool:
        statement = (
            select(ingestion_runs.c.run_id)
            .where(
                ingestion_runs.c.dataset_id == dataset_id,
                ingestion_runs.c.requested_start == start_date.isoformat(),
                ingestion_runs.c.requested_end == end_date.isoformat(),
                ingestion_runs.c.bbox == bbox_value,
                ingestion_runs.c.status == "COMPLETED",
            )
            .limit(1)
        )
        with self.engine.connect() as connection:
            return connection.execute(statement).first() is not None

    def create_run(
        self,
        dataset_id: str,
        start_date: date,
        end_date: date,
        bbox_value: str,
    ) -> int:
        statement = (
            ingestion_runs.insert()
            .values(
                dataset_id=dataset_id,
                requested_start=start_date.isoformat(),
                requested_end=end_date.isoformat(),
                bbox=bbox_value,
                started_at=datetime.now(timezone.utc),
                status="RUNNING",
            )
            .returning(ingestion_runs.c.run_id)
        )
        with self.engine.begin() as connection:
            run_id = connection.execute(statement).scalar_one()
        return int(run_id)

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        fetched: int = 0,
        inserted: int = 0,
        duplicates: int = 0,
        rejected: int = 0,
        source_path: str | None = None,
        source_sha256: str | None = None,
        error: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        statement = (
            update(ingestion_runs)
            .where(ingestion_runs.c.run_id == run_id)
            .values(
                completed_at=now,
                status=status,
                fetched_rows=fetched,
                inserted_rows=inserted,
                duplicate_rows=duplicates,
                rejected_rows=rejected,
                source_csv_path=source_path,
                source_sha256=source_sha256,
                error_message=error,
            )
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
            if result.rowcount == 0:
                raise KeyError(f"Ingestion run {run_id} does not exist")
            if source_path and source_sha256:
                path = Path(source_path)
                asset_values = {
                    "ingestion_run_id": run_id,
                    "asset_role": "FIRMS_SOURCE_SNAPSHOT",
                    "storage_uri": source_path,
                    "sha256": source_sha256,
                    "media_type": "text/csv",
                    "byte_size": path.stat().st_size if path.exists() else None,
                    "created_at": now,
                }
                if self.dialect_name == "sqlite":
                    asset_statement = sqlite_insert(raw_assets).values(asset_values)
                elif self.dialect_name == "postgresql":
                    asset_statement = postgresql_insert(raw_assets).values(asset_values)
                else:
                    raise RuntimeError(
                        f"Unsupported database dialect for RawAsset upsert: {self.dialect_name}"
                    )
                connection.execute(asset_statement.on_conflict_do_nothing(
                    index_elements=[raw_assets.c.ingestion_run_id, raw_assets.c.sha256]
                ))

    def insert_observations(
        self,
        run_id: int,
        normalized_observations: Iterable[Mapping[str, Any]],
    ) -> tuple[int, int]:
        """Insert normalized rows, returning accurate (inserted, duplicate) counts."""
        now = datetime.now(timezone.utc)
        rows = []
        for item in normalized_observations:
            row = dict(item)
            row["first_ingested_at"] = now
            row["ingestion_run_id"] = run_id
            rows.append(row)

        if not rows:
            return 0, 0
        inserted = 0
        with self.engine.begin() as connection:
            for offset in range(0, len(rows), INSERT_BATCH_SIZE):
                batch = rows[offset:offset + INSERT_BATCH_SIZE]
                if self.dialect_name == "sqlite":
                    statement = sqlite_insert(observations).values(batch)
                elif self.dialect_name == "postgresql":
                    statement = postgresql_insert(observations).values(batch)
                else:
                    raise RuntimeError(
                        f"Unsupported database dialect for conflict-safe inserts: {self.dialect_name}"
                    )
                statement = statement.on_conflict_do_nothing(
                    index_elements=[observations.c.source_row_hash]
                ).returning(observations.c.source_row_hash)
                inserted_hashes = connection.execute(statement).scalars().all()
                inserted += len(inserted_hashes)
        return inserted, len(rows) - inserted

    # ---- SKH §6.6 / §6.7 / §6.10 processing repository methods ----
    def create_processing_run(
        self,
        module_name: str,
        method_version: str,
        configuration_json: str,
    ) -> int:
        if module_name not in {"QC", "OPPORTUNITY", "MATCHING", "MEASUREMENT", "AGGREGATION"}:
            raise ValueError("unsupported processing module_name")
        statement = (
            processing_runs.insert()
            .values(
                module_name=module_name,
                method_version=method_version,
                configuration_json=configuration_json,
                started_at=datetime.now(timezone.utc),
                input_count=0,
                output_count=0,
                status="RUNNING",
            )
            .returning(processing_runs.c.processing_run_id)
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement).scalar_one()
        return int(result)

    def finish_processing_run(
        self,
        processing_run_id: int,
        *,
        status: str,
        input_count: int,
        output_count: int,
        error_message: str | None = None,
    ) -> None:
        if status not in {"COMPLETED", "FAILED", "PARTIAL"}:
            raise ValueError("status must be COMPLETED, FAILED, or PARTIAL")
        statement = (
            update(processing_runs)
            .where(processing_runs.c.processing_run_id == processing_run_id)
            .values(
                completed_at=datetime.now(timezone.utc),
                input_count=input_count,
                output_count=output_count,
                status=status,
                error_message=error_message,
            )
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
            if result.rowcount == 0:
                raise KeyError(f"Processing run {processing_run_id} does not exist")

    def fetch_unprocessed_observations(
        self,
        *,
        method_version: str,
        after_observation_id: int = 0,
        limit: int = 1000,
        reprocess: bool = False,
    ) -> list[dict[str, Any]]:
        """Keyset-page observations not QC-processed by this method version."""
        if limit < 1:
            raise ValueError("limit must be positive")
        statement = (
            select(observations)
            .where(observations.c.observation_id > after_observation_id)
            .order_by(observations.c.observation_id)
            .limit(limit)
        )
        if not reprocess:
            prior_qc = (
                select(qc_results.c.qc_result_id)
                .select_from(qc_results.join(
                    processing_runs,
                    qc_results.c.processing_run_id == processing_runs.c.processing_run_id,
                ))
                .where(
                    qc_results.c.observation_id == observations.c.observation_id,
                    processing_runs.c.module_name == "QC",
                    processing_runs.c.method_version == method_version,
                    processing_runs.c.status == "COMPLETED",
                )
            )
            statement = statement.where(~exists(prior_qc))
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def insert_qc_results(self, rows: Iterable[Mapping[str, Any]]) -> int:
        payload = [dict(row) for row in rows]
        if not payload:
            return 0
        now = datetime.now(timezone.utc)
        for row in payload:
            row.setdefault("created_at", now)
        with self.engine.begin() as connection:
            for offset in range(0, len(payload), INSERT_BATCH_SIZE):
                connection.execute(qc_results.insert(), payload[offset:offset + INSERT_BATCH_SIZE])
        return len(payload)

    def fetch_observations_for_opportunity(
        self,
        *,
        sensor: str,
        west: float,
        south: float,
        east: float,
        north: float,
        start_at: datetime,
        end_at: datetime,
        qc_processing_run_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return observations with their latest completed QC result, if any."""
        if qc_processing_run_id is None:
            latest_qc_id = (
                select(func.max(qc_results.c.qc_result_id))
                .select_from(qc_results.join(
                    processing_runs,
                    qc_results.c.processing_run_id == processing_runs.c.processing_run_id,
                ))
                .where(
                    qc_results.c.observation_id == observations.c.observation_id,
                    processing_runs.c.module_name == "QC",
                    processing_runs.c.status == "COMPLETED",
                )
                .correlate(observations)
                .scalar_subquery()
            )
            qc_join = qc_results.c.qc_result_id == latest_qc_id
        else:
            qc_join = and_(
                qc_results.c.observation_id == observations.c.observation_id,
                qc_results.c.processing_run_id == qc_processing_run_id,
            )

        selected_columns = [observations.c[name] for name in observations.c.keys()]
        selected_columns.extend([
            qc_results.c.qc_result_id.label("qc_result_id"),
            qc_results.c.processing_run_id.label("qc_processing_run_id"),
            qc_results.c.quality_status.label("qc_quality_status"),
            qc_results.c.suitability_for_matching.label("suitability_for_matching"),
            qc_results.c.suitability_for_frp.label("suitability_for_frp"),
            qc_results.c.warnings_json.label("qc_warnings_json"),
            qc_results.c.source_confidence_category.label("source_confidence_category"),
        ])
        statement = (
            select(*selected_columns)
            .select_from(observations.outerjoin(qc_results, qc_join))
            .where(
                observations.c.sensor == sensor,
                observations.c.longitude >= west,
                observations.c.longitude <= east,
                observations.c.latitude >= south,
                observations.c.latitude <= north,
                observations.c.acquired_at >= start_at,
                observations.c.acquired_at < end_at,
            )
            .order_by(observations.c.acquired_at, observations.c.observation_id)
        )
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def insert_opportunities(self, rows: Iterable[Mapping[str, Any]]) -> int:
        payload = [dict(row) for row in rows]
        if not payload:
            return 0
        now = datetime.now(timezone.utc)
        for row in payload:
            row.setdefault("created_at", now)
        with self.engine.begin() as connection:
            for offset in range(0, len(payload), INSERT_BATCH_SIZE):
                connection.execute(
                    observation_opportunities.insert(),
                    payload[offset:offset + INSERT_BATCH_SIZE],
                )
        return len(payload)

    def get_opportunity_summary(
        self,
        *,
        sensor: str,
        spatial_unit_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> dict[str, Any]:
        """Summarize the newest completed opportunity run for the exact query."""
        latest_run = (
            select(func.max(observation_opportunities.c.processing_run_id))
            .select_from(observation_opportunities.join(
                processing_runs,
                observation_opportunities.c.processing_run_id == processing_runs.c.processing_run_id,
            ))
            .where(
                observation_opportunities.c.sensor == sensor,
                observation_opportunities.c.spatial_unit_id == spatial_unit_id,
                observation_opportunities.c.time_window_start == start_at,
                observation_opportunities.c.time_window_end == end_at,
                processing_runs.c.module_name == "OPPORTUNITY",
                processing_runs.c.status == "COMPLETED",
            )
            .scalar_subquery()
        )
        statement = (
            select(
                observation_opportunities.c.opportunity_state,
                func.count().label("record_count"),
            )
            .where(observation_opportunities.c.processing_run_id == latest_run)
            .group_by(observation_opportunities.c.opportunity_state)
        )
        with self.engine.connect() as connection:
            rows = connection.execute(statement).all()
        return {row.opportunity_state: int(row.record_count) for row in rows}

    def fetch_scientific_observations(
        self,
        *,
        sensors: Iterable[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        west: float | None = None,
        south: float | None = None,
        east: float | None = None,
        north: float | None = None,
        qc_processing_run_id: int | None = None,
        require_matching_suitability: bool = True,
        require_frp_suitability: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch observations with a completed QC result; no source rows are mutated."""
        qc_subquery = (
            select(func.max(qc_results.c.qc_result_id))
            .select_from(qc_results.join(
                processing_runs,
                qc_results.c.processing_run_id == processing_runs.c.processing_run_id,
            ))
            .where(
                qc_results.c.observation_id == observations.c.observation_id,
                processing_runs.c.module_name == "QC",
                processing_runs.c.status == "COMPLETED",
            )
        )
        if qc_processing_run_id is not None:
            qc_subquery = qc_subquery.where(qc_results.c.processing_run_id == qc_processing_run_id)
        latest_qc_id = qc_subquery.correlate(observations).scalar_subquery()
        qc_join_condition = qc_results.c.qc_result_id == latest_qc_id
        selected = [observations.c[name] for name in observations.c.keys()]
        selected.extend([
            qc_results.c.qc_result_id.label("qc_result_id"),
            qc_results.c.processing_run_id.label("qc_processing_run_id"),
            qc_results.c.quality_status.label("qc_quality_status"),
            qc_results.c.suitability_for_matching.label("suitability_for_matching"),
            qc_results.c.suitability_for_frp.label("suitability_for_frp"),
            qc_results.c.warnings_json.label("qc_warnings_json"),
            qc_results.c.source_confidence_category.label("source_confidence_category"),
        ])
        statement = select(*selected).select_from(
            observations.join(qc_results, qc_join_condition)
        )
        if sensors:
            sensor_values = [str(value).upper() for value in sensors]
            statement = statement.where(observations.c.sensor.in_(sensor_values))
        if start_at is not None:
            statement = statement.where(observations.c.acquired_at >= start_at)
        if end_at is not None:
            statement = statement.where(observations.c.acquired_at < end_at)
        if west is not None:
            statement = statement.where(observations.c.longitude >= west)
        if east is not None:
            statement = statement.where(observations.c.longitude <= east)
        if south is not None:
            statement = statement.where(observations.c.latitude >= south)
        if north is not None:
            statement = statement.where(observations.c.latitude <= north)
        if require_matching_suitability:
            statement = statement.where(qc_results.c.suitability_for_matching.is_(True))
        if require_frp_suitability:
            statement = statement.where(qc_results.c.suitability_for_frp.is_(True))
        statement = statement.order_by(observations.c.acquired_at, observations.c.observation_id)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def fetch_observations_by_ids(self, observation_ids: Iterable[int]) -> list[dict[str, Any]]:
        ids = sorted({int(value) for value in observation_ids})
        if not ids:
            return []
        latest_qc_id = (
            select(func.max(qc_results.c.qc_result_id))
            .select_from(qc_results.join(
                processing_runs,
                qc_results.c.processing_run_id == processing_runs.c.processing_run_id,
            ))
            .where(
                qc_results.c.observation_id == observations.c.observation_id,
                processing_runs.c.module_name == "QC",
                processing_runs.c.status == "COMPLETED",
            )
            .correlate(observations)
            .scalar_subquery()
        )
        selected = [observations.c[name] for name in observations.c.keys()]
        selected.extend([
            qc_results.c.quality_status.label("qc_quality_status"),
            qc_results.c.suitability_for_matching.label("suitability_for_matching"),
            qc_results.c.suitability_for_frp.label("suitability_for_frp"),
        ])
        statement = (
            select(*selected)
            .select_from(observations.outerjoin(qc_results, qc_results.c.qc_result_id == latest_qc_id))
            .where(observations.c.observation_id.in_(ids))
            .order_by(observations.c.observation_id)
        )
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def insert_candidate_associations(self, rows: Iterable[Mapping[str, Any]]) -> int:
        payload = [dict(row) for row in rows]
        if not payload:
            return 0
        # candidate_associations is an evidence edge table; its timestamps are
        # represented by the linked processing_runs row, so it has no created_at.
        inserted = 0
        with self.engine.begin() as connection:
            for offset in range(0, len(payload), INSERT_BATCH_SIZE):
                batch = payload[offset:offset + INSERT_BATCH_SIZE]
                if self.dialect_name == "sqlite":
                    statement = sqlite_insert(candidate_associations).values(batch)
                elif self.dialect_name == "postgresql":
                    statement = postgresql_insert(candidate_associations).values(batch)
                else:
                    raise RuntimeError(f"Unsupported database dialect: {self.dialect_name}")
                statement = statement.on_conflict_do_nothing(
                    index_elements=[
                        candidate_associations.c.processing_run_id,
                        candidate_associations.c.matching_method,
                        candidate_associations.c.modis_observation_id,
                        candidate_associations.c.viirs_observation_id,
                    ]
                ).returning(candidate_associations.c.candidate_id)
                inserted += len(connection.execute(statement).scalars().all())
        return inserted

    def fetch_candidate_associations(self, processing_run_id: int) -> list[dict[str, Any]]:
        statement = (
            select(candidate_associations)
            .where(candidate_associations.c.processing_run_id == processing_run_id)
            .order_by(candidate_associations.c.candidate_id)
        )
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def insert_measurement_results(self, rows: Iterable[Mapping[str, Any]]) -> int:
        payload = [dict(row) for row in rows]
        if not payload:
            return 0
        now = datetime.now(timezone.utc)
        for row in payload:
            row.setdefault("created_at", now)
        with self.engine.begin() as connection:
            for offset in range(0, len(payload), INSERT_BATCH_SIZE):
                connection.execute(measurement_results.insert(), payload[offset:offset + INSERT_BATCH_SIZE])
        return len(payload)

    def fetch_measurement_results(self, processing_run_id: int) -> list[dict[str, Any]]:
        statement = select(measurement_results).where(
            measurement_results.c.processing_run_id == processing_run_id
        ).order_by(measurement_results.c.measurement_result_id)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def insert_aggregation_results(self, rows: Iterable[Mapping[str, Any]]) -> int:
        """Insert aggregation rows in bounded batches without materializing all input."""
        inserted = 0
        batch: list[dict[str, Any]] = []
        with self.engine.begin() as connection:
            for item in rows:
                row = dict(item)
                row.setdefault("created_at", datetime.now(timezone.utc))
                batch.append(row)
                if len(batch) >= INSERT_BATCH_SIZE:
                    connection.execute(aggregation_results.insert(), batch)
                    inserted += len(batch)
                    batch.clear()
            if batch:
                connection.execute(aggregation_results.insert(), batch)
                inserted += len(batch)
        return inserted

    def fetch_aggregation_results(self, processing_run_id: int) -> list[dict[str, Any]]:
        statement = select(aggregation_results).where(
            aggregation_results.c.processing_run_id == processing_run_id
        ).order_by(aggregation_results.c.period_start, aggregation_results.c.grid_cell_id)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def list_processing_runs(
        self, *, limit: int = 20, module_name: str | None = None
    ) -> list[dict[str, Any]]:
        """List recent processing runs, useful for recovering IDs after CLI runs."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        statement = select(processing_runs)
        if module_name is not None:
            statement = statement.where(processing_runs.c.module_name == module_name)
        statement = statement.order_by(processing_runs.c.processing_run_id.desc()).limit(limit)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(statement).mappings().all()]

    def get_processing_run(self, processing_run_id: int) -> dict[str, Any] | None:
        statement = select(processing_runs).where(
            processing_runs.c.processing_run_id == processing_run_id
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def count_observations(self) -> int:
        from sqlalchemy import func
        with self.engine.connect() as connection:
            return int(connection.execute(select(func.count()).select_from(observations)).scalar_one())

    def get_observation_by_hash(self, source_row_hash: str) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(observations).where(observations.c.source_row_hash == source_row_hash)
            ).mappings().first()
            return dict(row) if row else None


def connect_db(database_url: str | Path | None = None) -> DatabaseRepository:
    """Create/initialize the repository using DATABASE_URL or SQLite default."""
    resolved_url = resolve_database_url(database_url)
    url = make_url(resolved_url)
    connect_args: dict[str, Any] = {}
    if url.get_backend_name() == "sqlite":
        connect_args["check_same_thread"] = False

    engine = create_engine(
        resolved_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    if url.get_backend_name() == "sqlite":
        event.listen(engine, "connect", _sqlite_connection_pragmas)

    try:
        # Create base tables with checkfirst, then add new observation columns
        # before creating indexes that refer to those columns. This is safe for
        # both fresh databases and the earlier SQLite schema.
        metadata.create_all(engine, tables=[ingestion_runs, raw_assets])
        if "observations" not in inspect(engine).get_table_names():
            observations.create(engine)
        else:
            _migrate_additive_observation_columns(engine)
        _migrate_postgresql_timestamp_columns(engine)
        metadata.create_all(engine, tables=[processing_runs])
        _migrate_processing_run_modules(engine)
        metadata.create_all(engine, tables=[
            qc_results, observation_opportunities, candidate_associations,
            measurement_results, aggregation_results,
        ])
        for index in (
            idx_observations_acquired_at,
            idx_observations_dataset_time,
            idx_observations_sensor_collection,
            idx_observations_lat_lon,
            idx_runs_dataset_status,
            idx_raw_assets_sha256,
            idx_processing_runs_module_method,
            idx_qc_results_observation,
            idx_qc_results_run,
            idx_opportunities_lookup,
            idx_candidate_run,
            idx_candidate_modis,
            idx_candidate_viirs,
            idx_measurement_run,
            idx_measurement_observation,
            idx_aggregation_run,
            idx_aggregation_grid_period,
        ):
            index.create(engine, checkfirst=True)
        _backfill_raw_assets(engine)
    except Exception:
        engine.dispose()
        raise
    return DatabaseRepository(engine)


def build_observation_upsert_statement(
    dialect_name: str,
    rows: list[dict[str, Any]],
):
    """Build an upsert statement; exposed to test PostgreSQL SQL compilation."""
    if dialect_name == "sqlite":
        statement = sqlite_insert(observations).values(rows)
    elif dialect_name == "postgresql":
        statement = postgresql_insert(observations).values(rows)
    else:
        raise ValueError(f"Unsupported dialect: {dialect_name}")
    return statement.on_conflict_do_nothing(
        index_elements=[observations.c.source_row_hash]
    ).returning(observations.c.source_row_hash)
