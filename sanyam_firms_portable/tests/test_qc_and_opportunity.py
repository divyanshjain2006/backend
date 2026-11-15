import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select, update, inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db import (
    connect_db,
    processing_runs,
    qc_results,
    observation_opportunities,
    observations,
)
from firms_ingest import normalize_row
from qc_and_opportunity import (
    FIRMS_UNKNOWN_NOTE,
    BoundingBox,
    FIRMSDetectionOnlyProvider,
    evaluate_observation_opportunity,
    evaluate_observation_qc,
    query_opportunity_summary,
    run_quality_control,
)


MODIS_ROW = {
    "latitude": "26.9124", "longitude": "75.7873",
    "acq_date": "2024-01-02", "acq_time": "1234",
    "satellite": "T", "instrument": "MODIS", "confidence": "90",
    "version": "6.1SP", "frp": "15.2", "brightness": "320.1",
    "scan": "1.0", "track": "1.2", "daynight": "D",
}
VIIRS_ROW = {
    "latitude": "26.9130", "longitude": "75.7880",
    "acq_date": "2024-01-02", "acq_time": "1235",
    "satellite": "N20", "instrument": "VIIRS", "confidence": "n",
    "version": "2.0SP", "frp": "12.5", "bright_ti4": "330.2",
    "bright_ti5": "290.1", "scan": "0.4", "track": "0.5", "daynight": "D",
}


class RepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.repository = connect_db(Path(self.tempdir.name) / "sanyam.sqlite3")

    def tearDown(self):
        self.repository.close()
        self.tempdir.cleanup()

    def add_observation(self, dataset_id="MODIS_SP", source_row=None):
        row = dict(source_row or (MODIS_ROW if dataset_id.startswith("MODIS") else VIIRS_ROW))
        run_id = self.repository.create_run(
            dataset_id,
            date(2024, 1, 2),
            date(2024, 1, 2),
            "73,26,78,30",
        )
        normalized = normalize_row(dataset_id, row)
        inserted, duplicates = self.repository.insert_observations(run_id, [normalized])
        self.assertEqual((inserted, duplicates), (1, 0))
        stored = self.repository.get_observation_by_hash(normalized["source_row_hash"])
        return stored["observation_id"]

    def get_qc_for_run(self, run_id):
        with self.repository.engine.connect() as connection:
            return connection.execute(
                select(qc_results).where(qc_results.c.processing_run_id == run_id)
            ).mappings().all()

    def get_opportunities_for_run(self, run_id):
        with self.repository.engine.connect() as connection:
            return connection.execute(
                select(observation_opportunities).where(
                    observation_opportunities.c.processing_run_id == run_id
                )
            ).mappings().all()


class TestQualityControl(RepositoryTestCase):
    def test_valid_modis_row(self):
        observation_id = self.add_observation("MODIS_SP")
        result = run_quality_control(self.repository)
        rows = self.get_qc_for_run(result["processing_run_id"])
        self.assertEqual(result["input_count"], 1)
        self.assertEqual(result["output_count"], 1)
        self.assertEqual(rows[0]["observation_id"], observation_id)
        self.assertEqual(rows[0]["quality_status"], "VALID")
        self.assertEqual(rows[0]["source_confidence_category"], "HIGH")
        self.assertTrue(rows[0]["suitability_for_matching"])
        self.assertTrue(rows[0]["suitability_for_frp"])

    def test_low_confidence_modis_generates_warning(self):
        self.add_observation("MODIS_SP", dict(MODIS_ROW, confidence="29"))
        result = run_quality_control(self.repository)
        row = self.get_qc_for_run(result["processing_run_id"])[0]
        warnings = json.loads(row["warnings_json"])
        self.assertEqual(row["quality_status"], "VALID_WITH_WARNINGS")
        self.assertEqual(row["source_confidence_category"], "LOW")
        self.assertIn("LOW_CONFIDENCE_MODIS", {item["code"] for item in warnings})

    def test_modis_confidence_boundaries(self):
        for confidence, expected in (("30", "NOMINAL"), ("79", "NOMINAL"), ("80", "HIGH")):
            row = dict(MODIS_ROW, confidence=confidence)
            evaluation = evaluate_observation_qc({
                "observation_id": 1, "sensor": "MODIS", "dataset_id": "MODIS_SP",
                "processing_type": "SP", "latitude": 10, "longitude": 20,
                "acquired_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                "scan": 1, "track": 1, "frp_mw": 2,
                "confidence_raw": row["confidence"], "raw_json": "{}",
            })
            self.assertEqual(evaluation["source_confidence_category"], expected)

    def test_low_confidence_viirs_generates_warning(self):
        self.add_observation("VIIRS_NOAA20_SP", dict(VIIRS_ROW, confidence="l"))
        result = run_quality_control(self.repository)
        row = self.get_qc_for_run(result["processing_run_id"])[0]
        warnings = json.loads(row["warnings_json"])
        self.assertEqual(row["quality_status"], "VALID_WITH_WARNINGS")
        self.assertEqual(row["source_confidence_category"], "LOW")
        self.assertIn("LOW_CONFIDENCE_VIIRS", {item["code"] for item in warnings})

    def test_viirs_confidence_labels_map_case_insensitively(self):
        expected = {"l": "LOW", "LOW": "LOW", "n": "NOMINAL", "Nominal": "NOMINAL", "h": "HIGH", "HIGH": "HIGH"}
        for source, category in expected.items():
            evaluation = evaluate_observation_qc({
                "observation_id": 1, "sensor": "VIIRS", "dataset_id": "VIIRS_SNPP_SP",
                "processing_type": "SP", "latitude": 10, "longitude": 20,
                "acquired_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                "scan": 1, "track": 1, "frp_mw": 2,
                "confidence_raw": source, "raw_json": "{}",
            })
            self.assertEqual(evaluation["source_confidence_category"], category)

    def test_missing_frp_keeps_matching_suitability(self):
        self.add_observation("VIIRS_NOAA20_SP", dict(VIIRS_ROW, frp=""))
        result = run_quality_control(self.repository)
        row = self.get_qc_for_run(result["processing_run_id"])[0]
        warnings = json.loads(row["warnings_json"])
        self.assertEqual(row["quality_status"], "VALID_WITH_WARNINGS")
        self.assertTrue(row["suitability_for_matching"])
        self.assertFalse(row["suitability_for_frp"])
        self.assertIn("MISSING_OR_NONPOSITIVE_FRP", {item["code"] for item in warnings})

    def test_nonpositive_frp_disables_only_frp_suitability(self):
        result = evaluate_observation_qc({
            "observation_id": 1, "sensor": "MODIS", "dataset_id": "MODIS_SP",
            "processing_type": "SP", "latitude": 10, "longitude": 20,
            "acquired_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "scan": 1, "track": 1, "frp_mw": 0,
            "confidence_raw": "85", "raw_json": "{}",
        })
        self.assertTrue(result["suitability_for_matching"])
        self.assertFalse(result["suitability_for_frp"])

    def test_nrt_record_is_provisional_warning(self):
        self.add_observation("VIIRS_NOAA20_NRT", dict(VIIRS_ROW, version="2.0NRT"))
        result = run_quality_control(self.repository)
        row = self.get_qc_for_run(result["processing_run_id"])[0]
        warnings = json.loads(row["warnings_json"])
        self.assertEqual(row["quality_status"], "VALID_WITH_WARNINGS")
        self.assertIn("PROVISIONAL_NRT_RECORD", {item["code"] for item in warnings})

    def test_invalid_coordinates_are_preserved_and_unsuitable_for_matching(self):
        observation_id = self.add_observation()
        with self.repository.engine.begin() as connection:
            connection.execute(update(observations).where(
                observations.c.observation_id == observation_id
            ).values(latitude=95.0))
        result = run_quality_control(self.repository)
        row = self.get_qc_for_run(result["processing_run_id"])[0]
        self.assertEqual(row["quality_status"], "INVALID")
        self.assertFalse(row["suitability_for_matching"])
        self.assertFalse(row["suitability_for_frp"])
        self.assertEqual(self.repository.count_observations(), 1)

    def test_nonpositive_scan_track_are_warned_without_blocking_point_match(self):
        evaluation = evaluate_observation_qc({
            "observation_id": 1, "sensor": "MODIS", "dataset_id": "MODIS_SP",
            "processing_type": "SP", "latitude": 10, "longitude": 20,
            "acquired_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "scan": 0, "track": -1, "frp_mw": 2,
            "confidence_raw": "85", "raw_json": "{}",
        })
        self.assertEqual(evaluation["quality_status"], "VALID_WITH_WARNINGS")
        self.assertTrue(evaluation["suitability_for_matching"])
        codes = {item["code"] for item in json.loads(evaluation["warnings_json"])}
        self.assertTrue({"NONPOSITIVE_SCAN", "NONPOSITIVE_TRACK"}.issubset(codes))

    def test_future_acquisition_relative_to_ingestion_is_invalid(self):
        evaluation = evaluate_observation_qc({
            "observation_id": 1, "sensor": "MODIS", "dataset_id": "MODIS_SP",
            "processing_type": "SP", "latitude": 10, "longitude": 20,
            "acquired_at": datetime(2024, 1, 3, tzinfo=timezone.utc),
            "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "scan": 1, "track": 1, "frp_mw": 2,
            "confidence_raw": "85", "raw_json": "{}",
        })
        self.assertEqual(evaluation["quality_status"], "INVALID")
        self.assertFalse(evaluation["suitability_for_matching"])

    def test_malformed_raw_json_is_quarantined(self):
        evaluation = evaluate_observation_qc({
            "observation_id": 1, "sensor": "MODIS", "dataset_id": "MODIS_SP",
            "processing_type": "SP", "latitude": 10, "longitude": 20,
            "acquired_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "first_ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "scan": 1, "track": 1, "frp_mw": 2,
            "confidence_raw": "85", "raw_json": "not-json",
        })
        self.assertEqual(evaluation["quality_status"], "QUARANTINED")
        self.assertFalse(evaluation["suitability_for_matching"])

    def test_only_unprocessed_rows_are_qc_processed_by_version(self):
        self.add_observation("MODIS_SP")
        first = run_quality_control(self.repository, method_version="1.0.0")
        second = run_quality_control(self.repository, method_version="1.0.0")
        third = run_quality_control(self.repository, method_version="1.1.0")
        self.assertEqual(first["output_count"], 1)
        self.assertEqual(second["output_count"], 0)
        self.assertEqual(third["output_count"], 1)

    def test_reprocess_creates_new_immutable_qc_result(self):
        self.add_observation("MODIS_SP")
        first = run_quality_control(self.repository)
        second = run_quality_control(self.repository, reprocess=True)
        self.assertNotEqual(first["processing_run_id"], second["processing_run_id"])
        self.assertEqual(len(self.get_qc_for_run(first["processing_run_id"])), 1)
        self.assertEqual(len(self.get_qc_for_run(second["processing_run_id"])), 1)


class TestObservationOpportunity(RepositoryTestCase):
    def test_valid_detection_is_detected_and_linked_to_observation(self):
        observation_id = self.add_observation("VIIRS_NOAA20_SP")
        qc = run_quality_control(self.repository)
        result = evaluate_observation_opportunity(
            self.repository,
            bbox="73,26,78,30",
            start_date="2024-01-02",
            end_date="2024-01-02",
            sensor="VIIRS",
            qc_processing_run_id=qc["processing_run_id"],
        )
        rows = self.get_opportunities_for_run(result["processing_run_id"])
        self.assertEqual(result["state_counts"], {"DETECTED": 1})
        self.assertEqual(rows[0]["opportunity_state"], "DETECTED")
        self.assertEqual(rows[0]["observation_id"], observation_id)
        self.assertEqual(rows[0]["platform"], "NOAA-20")
        self.assertEqual(rows[0]["evidence_source"], "FIRMS_DETECTION_ONLY")

    def test_zero_firms_detections_are_unknown_never_valid_no_detection(self):
        result = evaluate_observation_opportunity(
            self.repository,
            bbox="73,26,78,30",
            start_date="2024-01-02",
            end_date="2024-01-03",
            sensor="MODIS",
        )
        rows = self.get_opportunities_for_run(result["processing_run_id"])
        self.assertEqual(result["state_counts"], {"UNKNOWN": 1})
        self.assertEqual(rows[0]["opportunity_state"], "UNKNOWN")
        self.assertEqual(rows[0]["evidence_source"], "FIRMS_DETECTION_ONLY")
        self.assertEqual(rows[0]["notes"], FIRMS_UNKNOWN_NOTE)
        self.assertNotIn("VALID_NO_DETECTION", result["state_counts"])

    def test_detection_without_qc_is_unknown_not_detected(self):
        self.add_observation("MODIS_SP")
        result = evaluate_observation_opportunity(
            self.repository,
            bbox="73,26,78,30",
            start_date="2024-01-02",
            end_date="2024-01-02",
            sensor="MODIS",
        )
        rows = self.get_opportunities_for_run(result["processing_run_id"])
        self.assertEqual(result["state_counts"], {"UNKNOWN": 1})
        self.assertIn("no completed QC", rows[0]["notes"])

    def test_invalid_qc_detection_is_invalid_quality(self):
        observation_id = self.add_observation("MODIS_SP")
        with self.repository.engine.begin() as connection:
            connection.execute(update(observations).where(
                observations.c.observation_id == observation_id
            ).values(first_ingested_at=datetime(2023, 1, 1, tzinfo=timezone.utc)))
        qc = run_quality_control(self.repository)
        result = evaluate_observation_opportunity(
            self.repository,
            bbox="73,26,78,30",
            start_date="2024-01-02",
            end_date="2024-01-02",
            sensor="MODIS",
            qc_processing_run_id=qc["processing_run_id"],
        )
        self.assertEqual(result["state_counts"], {"INVALID_QUALITY": 1})

    def test_opportunity_summary_returns_latest_run_states(self):
        self.add_observation("MODIS_SP")
        qc = run_quality_control(self.repository)
        args = dict(
            bbox="73,26,78,30", start_date="2024-01-02", end_date="2024-01-02",
            sensor="MODIS", qc_processing_run_id=qc["processing_run_id"],
        )
        evaluate_observation_opportunity(self.repository, **args)
        summary = query_opportunity_summary(
            self.repository, bbox=args["bbox"], start_date=args["start_date"],
            end_date=args["end_date"], sensor=args["sensor"],
        )
        self.assertEqual(summary, {"DETECTED": 1})

    def test_future_level2_provider_can_emit_valid_non_detection(self):
        class FakeLevel2Provider:
            evidence_source = "LEVEL2_SWATH_MASK"

            def build_records(self, observations, **context):
                return [{
                    "processing_run_id": context["processing_run_id"],
                    "observation_id": None,
                    "sensor": context["sensor"],
                    "platform": "Terra",
                    "spatial_unit_id": context["spatial_unit_id"],
                    "time_window_start": context["start_at"],
                    "time_window_end": context["end_at"],
                    "opportunity_state": "VALID_NO_DETECTION",
                    "evidence_source": self.evidence_source,
                    "notes": "Level-2 swath mask confirms valid observation with no fire classification",
                }]

        result = evaluate_observation_opportunity(
            self.repository,
            bbox="73,26,78,30",
            start_date="2024-01-02",
            end_date="2024-01-02",
            sensor="MODIS",
            provider=FakeLevel2Provider(),
        )
        rows = self.get_opportunities_for_run(result["processing_run_id"])
        self.assertEqual(result["state_counts"], {"VALID_NO_DETECTION": 1})
        self.assertEqual(rows[0]["evidence_source"], "LEVEL2_SWATH_MASK")

    def test_bbox_and_date_validation(self):
        with self.assertRaises(ValueError):
            BoundingBox.parse("73,30,78,26")
        with self.assertRaises(ValueError):
            evaluate_observation_opportunity(
                self.repository, bbox="73,26,78,30", start_date="2024-01-03",
                end_date="2024-01-02", sensor="MODIS",
            )

    def test_db_schema_contains_module_four_and_five_tables(self):
        table_names = set(inspect(self.repository.engine).get_table_names())
        self.assertTrue({"processing_runs", "qc_results", "observation_opportunities"}.issubset(table_names))
        qc_columns = {column["name"] for column in inspect(self.repository.engine).get_columns("qc_results")}
        self.assertTrue({
            "observation_id", "processing_run_id", "quality_status", "suitability_for_matching",
            "suitability_for_frp", "warnings_json", "source_confidence_category",
        }.issubset(qc_columns))

    def test_postgresql_ddl_has_timezone_and_constraints(self):
        qc_ddl = str(CreateTable(qc_results).compile(dialect=postgresql.dialect()))
        opp_ddl = str(CreateTable(observation_opportunities).compile(dialect=postgresql.dialect()))
        run_ddl = str(CreateTable(processing_runs).compile(dialect=postgresql.dialect()))
        self.assertIn("TIMESTAMP WITH TIME ZONE", run_ddl)
        self.assertIn("TIMESTAMP WITH TIME ZONE", opp_ddl)
        self.assertIn("suitability_for_matching", qc_ddl)
        self.assertIn("FIRMS_DETECTION_ONLY", opp_ddl)
        self.assertIn("GENERATED BY DEFAULT AS IDENTITY", run_ddl)


if __name__ == "__main__":
    unittest.main()
