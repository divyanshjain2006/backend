import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db import (
    DEFAULT_DATABASE_URL,
    DatabaseRepository,
    UTCDateTime,
    build_observation_upsert_statement,
    connect_db,
    ingestion_runs,
    observations,
    raw_assets,
    resolve_database_url,
)
from firms_ingest import (
    Availability,
    BBox,
    QuotaBudgetReached,
    SOURCE_FAMILIES,
    acq_datetime_utc,
    build_auto_plan,
    check_transaction_headroom,
    collection_from_version,
    date_chunks,
    get_map_key_status,
    intersect_dates,
    ingest_dataset,
    normalize_row,
    normalize_source_rows,
    save_source_snapshot,
    sensor_from_dataset_id,
    stable_row_hash,
    status_transaction_counts,
)


SAMPLE_VIIRS_ROW = {
    "latitude": "26.9124",
    "longitude": "75.7873",
    "acq_date": "2024-01-02",
    "acq_time": "1234",
    "satellite": "N20",
    "instrument": "VIIRS",
    "confidence": "n",
    "version": "2.0SP",
    "bright_ti4": "330.2",
    "bright_ti5": "290.1",
    "frp": "12.5",
    "scan": "0.4",
    "track": "0.5",
    "daynight": "D",
}


class TestFirmsIngestion(unittest.TestCase):
    def test_bbox_parse(self):
        bbox = BBox.parse("73,26,78,30")
        self.assertIsInstance(bbox, BBox)
        self.assertEqual(bbox.as_api_value(), "73.0,26.0,78.0,30.0")
        self.assertEqual(BBox.parse("world"), "world")

    def test_date_chunks_never_exceed_five_days(self):
        chunks = list(date_chunks(date(2020, 1, 1), date(2020, 1, 12)))
        self.assertEqual(chunks, [
            (date(2020, 1, 1), 5),
            (date(2020, 1, 6), 5),
            (date(2020, 1, 11), 2),
        ])

    def test_availability_intersection(self):
        available = Availability("VIIRS_SNPP_SP", date(2012, 1, 20), date(2025, 2, 28))
        self.assertEqual(
            intersect_dates(date(2010, 1, 1), date(2013, 1, 1), available),
            (date(2012, 1, 20), date(2013, 1, 1)),
        )
        self.assertIsNone(intersect_dates(date(2010, 1, 1), date(2011, 1, 1), available))

    def test_acquisition_time_is_timezone_aware_utc(self):
        result = acq_datetime_utc({"acq_date": "2024-01-02", "acq_time": "5"})
        self.assertEqual(result.isoformat(), "2024-01-02T00:05:00+00:00")
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_normalization_adds_skh_fields_and_retains_native_values(self):
        item = normalize_row("VIIRS_NOAA20_SP", SAMPLE_VIIRS_ROW)
        self.assertEqual(item["processing_type"], "SP")
        self.assertEqual(item["sensor"], "VIIRS")
        self.assertEqual(item["collection"], "2")
        self.assertEqual(item["confidence_raw"], "n")
        self.assertEqual(item["bright_ti4_k"], 330.2)
        self.assertEqual(item["frp_mw"], 12.5)
        self.assertEqual(item["footprint_source"], "FIRMS_POINT_SCAN_TRACK")
        self.assertEqual(item["opportunity_state"], "UNKNOWN")
        self.assertEqual(item["acquired_at"].tzinfo, timezone.utc)
        self.assertIn('"bright_ti4":"330.2"', item["raw_json"])

    def test_collection_is_parsed_without_inventing_unknown_values(self):
        self.assertEqual(collection_from_version("6.1SP"), "6.1")
        self.assertEqual(collection_from_version("6.1NRT"), "6.1")
        self.assertEqual(collection_from_version("2.0SP"), "2")
        self.assertIsNone(collection_from_version("unknown"))
        self.assertIsNone(collection_from_version(""))

    def test_sensor_is_mapped_from_firms_dataset_id(self):
        self.assertEqual(sensor_from_dataset_id("MODIS_SP"), "MODIS")
        self.assertEqual(sensor_from_dataset_id("VIIRS_NOAA20_SP"), "VIIRS")
        with self.assertRaises(ValueError):
            sensor_from_dataset_id("UNRECOGNIZED_SP")

    def test_normalize_source_rows_counts_rejected_records(self):
        invalid = dict(SAMPLE_VIIRS_ROW, latitude="999")
        normalized, rejected = normalize_source_rows("VIIRS_NOAA20_SP", [SAMPLE_VIIRS_ROW, invalid])
        self.assertEqual(len(normalized), 1)
        self.assertEqual(rejected, 1)

    def test_hash_is_stable_and_dataset_specific(self):
        row = {"latitude": "1", "longitude": "2"}
        self.assertEqual(stable_row_hash("MODIS_SP", row), stable_row_hash("MODIS_SP", row))
        self.assertNotEqual(stable_row_hash("MODIS_SP", row), stable_row_hash("MODIS_NRT", row))

    def test_sqlite_repository_insert_dedup_and_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = connect_db(Path(tmp) / "test.sqlite3")
            try:
                self.assertIsInstance(db, DatabaseRepository)
                run_id = db.create_run(
                    "VIIRS_NOAA20_SP", date(2024, 1, 2), date(2024, 1, 2), "73,26,78,30"
                )
                normalized = normalize_row("VIIRS_NOAA20_SP", SAMPLE_VIIRS_ROW)
                inserted, duplicates = db.insert_observations(run_id, [normalized, normalized])
                self.assertEqual((inserted, duplicates), (1, 1))
                self.assertEqual(db.count_observations(), 1)

                stored = db.get_observation_by_hash(normalized["source_row_hash"])
                self.assertIsNotNone(stored)
                self.assertEqual(stored["sensor"], "VIIRS")
                self.assertEqual(stored["collection"], "2")
                self.assertEqual(stored["footprint_source"], "FIRMS_POINT_SCAN_TRACK")
                self.assertEqual(stored["opportunity_state"], "UNKNOWN")
                self.assertEqual(stored["confidence_raw"], "n")
                self.assertIsNotNone(stored["acquired_at"].tzinfo)

                columns = {c["name"]: c for c in inspect(db.engine).get_columns("observations")}
                for name in ("sensor", "collection", "footprint_source", "opportunity_state"):
                    self.assertIn(name, columns)
                self.assertTrue(observations.c.acquired_at.type.impl.timezone)

                db.finish_run(run_id, status="COMPLETED", fetched=2, inserted=1, duplicates=1)
                self.assertTrue(db.has_completed_chunk(
                    "VIIRS_NOAA20_SP", date(2024, 1, 2), date(2024, 1, 2), "73,26,78,30"
                ))
            finally:
                db.close()

    def test_finish_run_persists_raw_asset_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = connect_db(Path(tmp) / "assets.sqlite3")
            try:
                run_id = db.create_run("MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "world")
                snapshot = Path(tmp) / "snapshot.csv"
                snapshot.write_text("latitude,longitude\n1,2\n", encoding="utf-8")
                import hashlib
                sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
                db.finish_run(run_id, status="COMPLETED", source_path=str(snapshot), source_sha256=sha)
                with db.engine.connect() as connection:
                    asset = connection.execute(
                        select(raw_assets).where(raw_assets.c.ingestion_run_id == run_id)
                    ).mappings().one()
                self.assertEqual(asset["sha256"], sha)
                self.assertEqual(asset["storage_uri"], str(snapshot))
            finally:
                db.close()

    def test_resume_detects_exact_completed_chunk(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = connect_db(Path(tmp) / "resume.sqlite3")
            try:
                run_id = db.create_run(
                    "MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "73,26,78,30"
                )
                self.assertFalse(db.has_completed_chunk(
                    "MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "73,26,78,30"
                ))
                db.finish_run(run_id, status="COMPLETED")
                self.assertTrue(db.has_completed_chunk(
                    "MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "73,26,78,30"
                ))
                self.assertFalse(db.has_completed_chunk(
                    "MODIS_SP", date(2024, 1, 2), date(2024, 1, 2), "73,26,78,30"
                ))
            finally:
                db.close()

    def test_old_sqlite_schema_is_upgraded_additively(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.sqlite3"
            engine = create_engine(f"sqlite:///{path}")
            legacy = MetaData()
            legacy_runs = Table(
                "ingestion_runs", legacy,
                Column("run_id", Integer, primary_key=True),
                Column("dataset_id", String(64), nullable=False),
                Column("requested_start", String(10), nullable=False),
                Column("requested_end", String(10), nullable=False),
                Column("bbox", String(128), nullable=False),
                Column("started_at", String, nullable=False),
                Column("completed_at", String),
                Column("status", String(32), nullable=False),
                Column("fetched_rows", Integer, nullable=False, default=0),
                Column("inserted_rows", Integer, nullable=False, default=0),
                Column("duplicate_rows", Integer, nullable=False, default=0),
                Column("rejected_rows", Integer, nullable=False, default=0),
                Column("source_csv_path", Text),
                Column("source_sha256", String(64)),
                Column("error_message", Text),
            )
            legacy_observations = Table(
                "observations", legacy,
                Column("observation_id", Integer, primary_key=True),
                Column("source_row_hash", String(64), nullable=False, unique=True),
                Column("dataset_id", String(64), nullable=False),
                Column("processing_type", String(8), nullable=False),
                Column("acquired_at", String, nullable=False),
                Column("latitude", Float, nullable=False),
                Column("longitude", Float, nullable=False),
                Column("satellite", String(32)),
                Column("instrument", String(32)),
                Column("confidence_raw", String(64)),
                Column("frp_mw", Float),
                Column("brightness_k", Float),
                Column("bright_ti4_k", Float),
                Column("bright_ti5_k", Float),
                Column("scan", Float),
                Column("track", Float),
                Column("daynight", String(8)),
                Column("source_version", String(64)),
                Column("raw_json", Text, nullable=False),
                Column("first_ingested_at", String, nullable=False),
                Column("ingestion_run_id", Integer, ForeignKey("ingestion_runs.run_id"), nullable=False),
            )
            legacy.create_all(engine)
            with engine.begin() as connection:
                connection.execute(legacy_runs.insert().values(
                    run_id=1, dataset_id="VIIRS_NOAA20_SP",
                    requested_start="2024-01-02", requested_end="2024-01-02",
                    bbox="73,26,78,30", started_at="2024-01-02T12:40:00Z",
                    status="COMPLETED", fetched_rows=1, inserted_rows=1,
                    duplicate_rows=0, rejected_rows=0,
                ))
                connection.execute(legacy_observations.insert().values(
                    observation_id=1, source_row_hash="legacyhash",
                    dataset_id="VIIRS_NOAA20_SP", processing_type="SP",
                    acquired_at="2024-01-02T12:34:00Z", latitude=26.9, longitude=75.7,
                    satellite="N20", instrument="VIIRS", confidence_raw="n",
                    frp_mw=12.5, source_version="2.0SP", raw_json="{}",
                    first_ingested_at="2024-01-02T12:41:00Z", ingestion_run_id=1,
                ))
            engine.dispose()

            db = connect_db(path)
            try:
                columns = {c["name"] for c in inspect(db.engine).get_columns("observations")}
                self.assertTrue({"sensor", "collection", "footprint_source", "opportunity_state"}.issubset(columns))
                self.assertIn("raw_assets", inspect(db.engine).get_table_names())
                migrated = db.get_observation_by_hash("legacyhash")
                self.assertEqual(migrated["sensor"], "VIIRS")
                self.assertEqual(migrated["collection"], "2")
                self.assertEqual(migrated["footprint_source"], "FIRMS_POINT_SCAN_TRACK")
                self.assertEqual(migrated["opportunity_state"], "UNKNOWN")
                self.assertEqual(migrated["acquired_at"].tzinfo, timezone.utc)
            finally:
                db.close()

    def test_database_url_environment_and_postgresql_driver(self):
        with tempfile.TemporaryDirectory() as tmp:
            url = f"sqlite:///{tmp}/from_env.sqlite3"
            with patch.dict(os.environ, {"DATABASE_URL": url}):
                self.assertEqual(resolve_database_url(), url)
        self.assertTrue(
            resolve_database_url("postgresql://user:secret@localhost:5432/skh").startswith(
                "postgresql+psycopg://"
            )
        )
        self.assertEqual(resolve_database_url(), os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))

    def test_postgresql_upsert_uses_on_conflict_and_returning(self):
        statement = build_observation_upsert_statement("postgresql", [{"source_row_hash": "abc"}])
        compiled = str(statement.compile(dialect=postgresql.dialect()))
        self.assertIn("ON CONFLICT (source_row_hash) DO NOTHING", compiled)
        self.assertIn("RETURNING observations.source_row_hash", compiled)

    def test_postgresql_schema_uses_identity_and_timestamptz(self):
        ddl = str(CreateTable(observations).compile(dialect=postgresql.dialect()))
        self.assertIn("GENERATED BY DEFAULT AS IDENTITY", ddl)
        self.assertIn("TIMESTAMP WITH TIME ZONE", ddl)
        self.assertIn("footprint_source", ddl)
        self.assertIn("opportunity_state", ddl)


    def test_source_snapshot_uses_sha256_and_does_not_overwrite_changed_content(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp) / "raw"
            first_path, first_sha = save_source_snapshot(
                raw_dir, "MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "header\nrow-a\n"
            )
            second_path, second_sha = save_source_snapshot(
                raw_dir, "MODIS_SP", date(2024, 1, 1), date(2024, 1, 1), "header\nrow-b\n"
            )
            self.assertEqual(first_sha, hashlib.sha256(b"header\nrow-a\n").hexdigest())
            self.assertNotEqual(first_sha, second_sha)
            self.assertNotEqual(first_path, second_path)
            self.assertEqual(first_path.read_text(encoding="utf-8"), "header\nrow-a\n")
            self.assertEqual(second_path.read_text(encoding="utf-8"), "header\nrow-b\n")

    def test_ingest_dataset_persists_snapshot_observation_and_resume_marker(self):
        import firms_ingest
        csv_text = (
            "latitude,longitude,acq_date,acq_time,satellite,instrument,confidence,"
            "version,frp,scan,track,daynight\n"
            "26.9124,75.7873,2024-01-02,1234,N20,VIIRS,n,2.0SP,12.5,0.4,0.5,D\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            db = connect_db(Path(tmp) / "pipeline.sqlite3")
            try:
                with patch("firms_ingest.check_transaction_headroom", return_value=(10, 5000)), \
                     patch("firms_ingest.request_area_csv", return_value=csv_text):
                    totals = ingest_dataset(
                        session=object(), db=db, map_key="test-key",
                        dataset_id="VIIRS_NOAA20_SP",
                        start_date=date(2024, 1, 2), end_date=date(2024, 1, 2),
                        bbox=BBox.parse("73,26,78,30"), raw_dir=Path(tmp) / "raw",
                        keep_snapshots=True, chunk_days=1, transaction_reserve=1000,
                    )
                self.assertEqual(totals["inserted"], 1)
                self.assertEqual(totals["duplicates"], 0)
                self.assertEqual(db.count_observations(), 1)

                with patch("firms_ingest.check_transaction_headroom") as quota_check, \
                     patch("firms_ingest.request_area_csv") as data_request:
                    resumed = ingest_dataset(
                        session=object(), db=db, map_key="test-key",
                        dataset_id="VIIRS_NOAA20_SP",
                        start_date=date(2024, 1, 2), end_date=date(2024, 1, 2),
                        bbox=BBox.parse("73,26,78,30"), raw_dir=Path(tmp) / "raw",
                        keep_snapshots=True, chunk_days=1, transaction_reserve=1000,
                        resume=True,
                    )
                    quota_check.assert_not_called()
                    data_request.assert_not_called()
                self.assertEqual(resumed["skipped_completed"], 1)
            finally:
                db.close()

    def test_auto_plan_prefers_sp_and_uses_nrt_only_after_sp_ends(self):
        availability = {
            "MODIS_SP": Availability("MODIS_SP", date(2024, 1, 1), date(2024, 1, 3)),
            "MODIS_NRT": Availability("MODIS_NRT", date(2024, 1, 3), date(2024, 1, 6)),
        }
        plan = build_auto_plan(
            date(2024, 1, 1), date(2024, 1, 6), availability,
            families=["MODIS"], include_nrt_fallback=True,
        )
        self.assertEqual(plan, [
            ("MODIS_SP", date(2024, 1, 1), date(2024, 1, 3)),
            ("MODIS_NRT", date(2024, 1, 4), date(2024, 1, 6)),
        ])

    def test_auto_plan_sp_only_does_not_fallback_to_nrt(self):
        availability = {
            "MODIS_SP": Availability("MODIS_SP", date(2024, 1, 1), date(2024, 1, 3)),
            "MODIS_NRT": Availability("MODIS_NRT", date(2024, 1, 4), date(2024, 1, 6)),
        }
        plan = build_auto_plan(
            date(2024, 1, 1), date(2024, 1, 6), availability,
            families=["MODIS"], include_nrt_fallback=False,
        )
        self.assertEqual(plan, [("MODIS_SP", date(2024, 1, 1), date(2024, 1, 3))])

    def test_auto_plan_skips_unavailable_dates(self):
        availability = {
            "MODIS_SP": Availability("MODIS_SP", date(2024, 1, 1), date(2024, 1, 2)),
            "MODIS_NRT": Availability("MODIS_NRT", date(2024, 1, 5), date(2024, 1, 6)),
        }
        plan = build_auto_plan(
            date(2024, 1, 1), date(2024, 1, 6), availability,
            families=["MODIS"], include_nrt_fallback=True,
        )
        self.assertEqual(plan, [
            ("MODIS_SP", date(2024, 1, 1), date(2024, 1, 2)),
            ("MODIS_NRT", date(2024, 1, 5), date(2024, 1, 6)),
        ])

    def test_status_transaction_counts(self):
        self.assertEqual(
            status_transaction_counts({"current_transactions": "125", "transaction_limit": "5000"}),
            (125, 5000),
        )

    def test_known_sensor_families(self):
        self.assertEqual(SOURCE_FAMILIES["MODIS"], ("MODIS_SP", "MODIS_NRT"))
        self.assertIn("VIIRS_NOAA21", SOURCE_FAMILIES)

    def test_transaction_headroom_allows_request_below_safety_ceiling(self):
        with patch("firms_ingest.get_map_key_status", return_value={
            "current_transactions": 3500, "transaction_limit": 5000
        }):
            current, limit = check_transaction_headroom(object(), "fake-key", 1000)
        self.assertEqual((current, limit), (3500, 5000))

    def test_transaction_headroom_stops_at_safety_ceiling(self):
        with patch("firms_ingest.get_map_key_status", return_value={
            "current_transactions": 4000, "transaction_limit": 5000
        }):
            with self.assertRaises(QuotaBudgetReached):
                check_transaction_headroom(object(), "fake-key", 1000)


if __name__ == "__main__":
    unittest.main()
