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
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Identity,
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
        for index in (
            idx_observations_acquired_at,
            idx_observations_dataset_time,
            idx_observations_sensor_collection,
            idx_observations_lat_lon,
            idx_runs_dataset_status,
            idx_raw_assets_sha256,
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
