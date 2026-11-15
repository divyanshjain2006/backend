import json
import math
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, inspect, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db import (
    aggregation_results,
    candidate_associations,
    connect_db,
    measurement_results,
    processing_runs,
    qc_results,
)
from firms_ingest import normalize_row
from matching_and_measurement import (
    FIRMS_GEOMETRY_SOURCE,
    LEVEL2_GEOMETRY_SOURCE,
    POINT_FALLBACK_GEOMETRY_SOURCE,
    analyze_frp_measurements,
    build_common_grid_summary,
    build_equal_area_context,
    estimate_frp_detection_limit_mw,
    export_validation_geojson,
    footprint_metrics,
    haversine_distance_km,
    run_baseline_matching,
    run_footprint_matching,
    _build_parser,
)
from qc_and_opportunity import run_quality_control


class TestMatchingCLI(unittest.TestCase):
    def test_grid_cli_accepts_platform_and_data_source_options(self):
        args = _build_parser().parse_args([
            "grid",
            "--bbox", "73,26,78,30",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-05",
            "--resolution-km", "1.0",
            "--period-days", "1",
            "--matching-run-id", "8",
            "--platforms", "Terra,NOAA-20",
            "--data-source-mode", "LAADS_LEVEL2",
        ])
        self.assertEqual(args.command, "grid")
        self.assertEqual(args.matching_run_id, 8)
        self.assertEqual(args.platforms, "Terra,NOAA-20")
        self.assertEqual(args.data_source_mode, "LAADS_LEVEL2")


class SyntheticLevel2Provider:
    """Test double for the future Level-2 provider contract.

    It creates synthetic polygons solely to test provider injection and source
    tagging; it does not represent real DAAC pixel-corner extraction.
    """

    geometry_source = LEVEL2_GEOMETRY_SOURCE

    def build_polygon(self, observation, context):
        from shapely.geometry import box

        x, y = context.forward.transform(
            float(observation["longitude"]), float(observation["latitude"])
        )
        return box(x - 180.0, y - 180.0, x + 180.0, y + 180.0)


class CountingFRPOperator:
    name = "TEST_OPERATOR"
    version = "test-1"

    def __init__(self):
        self.calls = 0

    def adjust(self, observation, pixel_area_km2):
        self.calls += 1
        return float(observation["frp_mw"]) * 1.1


class MatchingRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.repository = connect_db(Path(self.tempdir.name) / "sanyam.sqlite3")

    def tearDown(self):
        self.repository.close()
        self.tempdir.cleanup()

    def add_observation(
        self,
        sensor="MODIS",
        *,
        lon=75.7873,
        lat=26.9124,
        minute=0,
        scan=1.0,
        track=1.0,
        frp=15.2,
        daynight="D",
        confidence=None,
    ):
        if sensor == "MODIS":
            dataset_id = "MODIS_SP"
            raw = {
                "latitude": str(lat), "longitude": str(lon),
                "acq_date": "2024-01-02", "acq_time": f"12{minute:02d}",
                "satellite": "T", "instrument": "MODIS",
                "confidence": str(confidence if confidence is not None else 90),
                "version": "6.1SP", "frp": str(frp) if frp is not None else "",
                "brightness": "320.1", "scan": str(scan) if scan is not None else "",
                "track": str(track) if track is not None else "", "daynight": daynight,
            }
        else:
            dataset_id = "VIIRS_NOAA20_SP"
            raw = {
                "latitude": str(lat), "longitude": str(lon),
                "acq_date": "2024-01-02", "acq_time": f"12{minute:02d}",
                "satellite": "N20", "instrument": "VIIRS",
                "confidence": str(confidence if confidence is not None else "n"),
                "version": "2.0SP", "frp": str(frp) if frp is not None else "",
                "bright_ti4": "330.2", "bright_ti5": "290.1",
                "scan": str(scan) if scan is not None else "",
                "track": str(track) if track is not None else "", "daynight": daynight,
            }
        ingest_run_id = self.repository.create_run(
            dataset_id, date(2024, 1, 2), date(2024, 1, 2), "test-bbox"
        )
        normalized = normalize_row(dataset_id, raw)
        inserted, duplicates = self.repository.insert_observations(ingest_run_id, [normalized])
        self.assertEqual((inserted, duplicates), (1, 0))
        stored = self.repository.get_observation_by_hash(normalized["source_row_hash"])
        return stored["observation_id"]

    def run_qc(self):
        return run_quality_control(self.repository)

    def fetch_candidates(self, run_id):
        return self.repository.fetch_candidate_associations(run_id)

    def test_haversine_known_distance_and_symmetry(self):
        self.assertAlmostEqual(haversine_distance_km(0, 0, 0, 1), 111.195, delta=0.02)
        self.assertAlmostEqual(haversine_distance_km(0, 0, 1, 0), haversine_distance_km(1, 0, 0, 0), places=10)
        self.assertEqual(haversine_distance_km(75, 27, 75, 27), 0.0)

    def test_footprint_metrics_small_pixel_inside_large_pixel(self):
        # Units are projected metres, not geographic degrees.
        large = __import__("shapely.geometry", fromlist=["box"]).box(0, 0, 1000, 1000)
        small = __import__("shapely.geometry", fromlist=["box"]).box(312.5, 312.5, 687.5, 687.5)
        metrics = footprint_metrics(large, small)
        self.assertAlmostEqual(metrics["intersection_area_km2"], 0.140625, places=6)
        self.assertLess(metrics["iou"], 0.15)
        self.assertAlmostEqual(metrics["smaller_footprint_overlap"], 1.0, places=8)
        self.assertAlmostEqual(metrics["boundary_distance_km"], 0.3125, places=6)

    def test_footprint_metrics_report_nearest_boundary_gap_for_nonoverlap(self):
        from shapely.geometry import box

        left = box(0, 0, 400, 400)
        right = box(600, 0, 1000, 400)
        metrics = footprint_metrics(left, right)
        self.assertEqual(metrics["intersection_area_km2"], 0.0)
        self.assertEqual(metrics["iou"], 0.0)
        self.assertEqual(metrics["smaller_footprint_overlap"], 0.0)
        self.assertAlmostEqual(metrics["boundary_distance_km"], 0.2, places=8)

    def test_equal_area_context_uses_projected_metric_crs(self):
        context = build_equal_area_context("75.7,26.8,75.9,27.0")
        self.assertIn("Lambert Azimuthal Equal Area", context.crs.coordinate_operation.method_name)
        self.assertGreater(context.study_polygon.area, 0)

    def test_baseline_many_to_many_graph_degrees_and_ambiguous_group(self):
        # 2 MODIS x 3 VIIRS nodes create M1-V1, M1-V2, M2-V2, M2-V3
        # under a 60 km threshold; M1-V3 and M2-V1 are outside it.
        m1 = self.add_observation("MODIS", lon=0.0, lat=0.0, minute=0)
        m2 = self.add_observation("MODIS", lon=1.0, lat=0.0, minute=0, frp=21.0)
        v1 = self.add_observation("VIIRS", lon=0.01, lat=0.0, minute=0)
        v2 = self.add_observation("VIIRS", lon=0.50, lat=0.0, minute=0, frp=11.0)
        v3 = self.add_observation("VIIRS", lon=0.99, lat=0.0, minute=0, frp=9.0)
        self.run_qc()
        result = run_baseline_matching(
            self.repository,
            start_date="2024-01-02", end_date="2024-01-02",
            max_distance_km=60.0, max_time_difference_minutes=30.0,
        )
        edges = [row for row in self.fetch_candidates(result["processing_run_id"])
                 if row["modis_observation_id"] is not None and row["viirs_observation_id"] is not None
                 and row["relationship_status"] != "REJECTED"]
        pairs = {(row["modis_observation_id"], row["viirs_observation_id"]) for row in edges}
        self.assertEqual(pairs, {(m1, v1), (m1, v2), (m2, v2), (m2, v3)})
        self.assertTrue(all(row["relationship_status"] == "AMBIGUOUS_GROUP" for row in edges))
        m1_edges = [row for row in edges if row["modis_observation_id"] == m1]
        self.assertTrue(all(row["modis_degree"] == 2 for row in m1_edges))
        v2_edges = [row for row in edges if row["viirs_observation_id"] == v2]
        self.assertTrue(all(row["viirs_degree"] == 2 for row in v2_edges))
        self.assertTrue(all(row["component_id"] for row in edges))

    def test_baseline_matching_filters_qc_unsuitable_and_records_unmatched(self):
        good_modis = self.add_observation("MODIS", lon=75.78, lat=26.91)
        far_viirs = self.add_observation("VIIRS", lon=76.5, lat=26.91)
        self.run_qc()
        result = run_baseline_matching(self.repository, start_date="2024-01-02", end_date="2024-01-02")
        records = self.fetch_candidates(result["processing_run_id"])
        self.assertEqual(result["candidate_edges"], 0)
        unmatched = [row for row in records if row["relationship_status"] == "UNMATCHED"]
        self.assertEqual(len(unmatched), 2)
        self.assertIn(good_modis, [row["modis_observation_id"] for row in unmatched])
        self.assertIn(far_viirs, [row["viirs_observation_id"] for row in unmatched])

    def test_footprint_matching_preserves_small_inside_large_overlap_metrics(self):
        m_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0)
        v_id = self.add_observation("VIIRS", lon=75.7873, lat=26.9124, scan=0.375, track=0.375)
        self.run_qc()
        result = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
            max_boundary_distance_km=0.1,
        )
        edges = [row for row in self.fetch_candidates(result["processing_run_id"])
                 if row["modis_observation_id"] == m_id and row["viirs_observation_id"] == v_id]
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(edge["geometry_source"], FIRMS_GEOMETRY_SOURCE)
        self.assertLess(edge["iou"], 0.15)
        self.assertGreater(edge["smaller_footprint_overlap"], 0.99)
        self.assertGreater(edge["intersection_area_km2"], 0)

    def test_footprint_matching_retains_near_boundary_nonoverlap_candidate(self):
        m_id = self.add_observation("MODIS", lon=75.7800, lat=26.9124, scan=0.4, track=0.4)
        # Roughly 0.6 km east at this latitude: 0.2 km gap between 0.4 km boxes.
        v_id = self.add_observation("VIIRS", lon=75.7862, lat=26.9124, scan=0.4, track=0.4)
        self.run_qc()
        result = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
            max_boundary_distance_km=0.25,
        )
        edges = [row for row in self.fetch_candidates(result["processing_run_id"])
                 if row["modis_observation_id"] == m_id and row["viirs_observation_id"] == v_id]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["relationship_status"], "SINGLE_CANDIDATE")
        self.assertEqual(edges[0]["intersection_area_km2"], 0.0)
        self.assertLessEqual(edges[0]["boundary_distance_km"], 0.25)

    def test_footprint_matching_uses_explicit_point_fallback_when_dimensions_missing(self):
        m_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1, track=1)
        v_id = self.add_observation("VIIRS", lon=75.7874, lat=26.9124, scan=None, track=None)
        self.run_qc()
        result = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
        )
        edges = [row for row in self.fetch_candidates(result["processing_run_id"])
                 if row["modis_observation_id"] == m_id and row["viirs_observation_id"] == v_id]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["geometry_source"], POINT_FALLBACK_GEOMETRY_SOURCE)
        self.assertIsNone(edges[0]["iou"])

    def test_point_fallback_is_created_when_modis_dimensions_are_missing(self):
        m_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=None, track=None)
        v_id = self.add_observation("VIIRS", lon=75.7874, lat=26.9124, scan=0.375, track=0.375)
        self.run_qc()
        result = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
        )
        edges = [row for row in self.fetch_candidates(result["processing_run_id"])
                 if row["modis_observation_id"] == m_id and row["viirs_observation_id"] == v_id]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["geometry_source"], POINT_FALLBACK_GEOMETRY_SOURCE)
        self.assertIsNone(edges[0]["intersection_area_km2"])

    def test_future_level2_footprint_provider_is_injectable_and_tagged(self):
        m_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0)
        v_id = self.add_observation("VIIRS", lon=75.7874, lat=26.9124, scan=0.375, track=0.375)
        self.run_qc()
        provider = SyntheticLevel2Provider()
        result = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
            footprint_provider=provider,
        )
        edge = next(row for row in self.fetch_candidates(result["processing_run_id"])
                    if row["modis_observation_id"] == m_id and row["viirs_observation_id"] == v_id)
        self.assertEqual(edge["geometry_source"], LEVEL2_GEOMETRY_SOURCE)
        self.assertEqual(result["geometry_source"], LEVEL2_GEOMETRY_SOURCE)
        with self.assertRaisesRegex(ValueError, "same FootprintProvider"):
            export_validation_geojson(
                self.repository, processing_run_id=result["processing_run_id"],
                output_dir=Path(self.tempdir.name) / "level2-export-mismatch",
            )
        paths = export_validation_geojson(
            self.repository, processing_run_id=result["processing_run_id"],
            output_dir=Path(self.tempdir.name) / "level2-export",
            footprint_provider=provider,
        )
        footprint_doc = json.loads(Path(paths["footprints.geojson"]).read_text())
        self.assertTrue(all(
            feature["properties"]["geometry_source"] == LEVEL2_GEOMETRY_SOURCE
            for feature in footprint_doc["features"]
        ))

    def test_frp_results_preserve_raw_and_compute_area_scaled_diagnostic_limits(self):
        m_id = self.add_observation("MODIS", scan=1.0, track=1.0, frp=15.2, daynight="D")
        v_id = self.add_observation("VIIRS", scan=0.375, track=0.375, frp=12.5, daynight="D")
        self.run_qc()
        result = analyze_frp_measurements(self.repository)
        with self.repository.engine.connect() as connection:
            rows = connection.execute(select(measurement_results).where(
                measurement_results.c.processing_run_id == result["processing_run_id"]
            )).mappings().all()
        by_obs = {row["observation_id"]: row for row in rows}
        self.assertEqual(by_obs[m_id]["frp_raw_mw"], 15.2)
        self.assertEqual(by_obs[v_id]["frp_raw_mw"], 12.5)
        self.assertIsNone(by_obs[m_id]["frp_adjusted_mw"])
        self.assertEqual(by_obs[m_id]["detection_limit_mw"], 5.7)
        self.assertAlmostEqual(by_obs[v_id]["detection_limit_mw"], 3.7, places=6)
        self.assertEqual(by_obs[m_id]["evidence_category"], "DIRECT_SOURCE_OBSERVATION")

    def test_frp_operator_is_not_applied_to_qc_unsuitable_observations(self):
        valid_id = self.add_observation("MODIS", frp=10.0, daynight="D")
        missing_frp_id = self.add_observation("VIIRS", frp=None, daynight="D")
        self.run_qc()
        operator = CountingFRPOperator()
        result = analyze_frp_measurements(self.repository, operator=operator)
        rows = self.repository.fetch_measurement_results(result["processing_run_id"])
        by_observation = {row["observation_id"]: row for row in rows}
        self.assertEqual(operator.calls, 1)
        self.assertAlmostEqual(by_observation[valid_id]["frp_adjusted_mw"], 11.0)
        self.assertIsNone(by_observation[missing_frp_id]["frp_raw_mw"])
        self.assertIsNone(by_observation[missing_frp_id]["frp_adjusted_mw"])
        self.assertEqual(by_observation[missing_frp_id]["evidence_category"], "DIRECT_SOURCE_OBSERVATION")

    def test_detection_limit_unknown_for_unknown_collection_or_daynight(self):
        base = {"sensor": "MODIS", "collection": "unknown", "daynight": "D"}
        self.assertIsNone(estimate_frp_detection_limit_mw(base, 1.0))
        base = {"sensor": "MODIS", "collection": "6.1", "daynight": "?"}
        self.assertIsNone(estimate_frp_detection_limit_mw(base, 1.0))

    def test_common_grid_uses_weighted_counts_and_firms_unknown_opportunity(self):
        self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0, frp=15.2)
        self.add_observation("VIIRS", lon=75.7873, lat=26.9124, scan=0.375, track=0.375, frp=12.5)
        self.run_qc()
        result = build_common_grid_summary(
            self.repository, bbox="75.75,26.87,75.83,26.95",
            start_date="2024-01-02", end_date="2024-01-02", grid_resolution_km=1.0,
        )
        rows = self.repository.fetch_aggregation_results(result["processing_run_id"])
        self.assertGreater(len(rows), 0)
        self.assertAlmostEqual(sum(row["modis_count"] for row in rows), 1.0, delta=0.02)
        self.assertAlmostEqual(sum(row["viirs_count"] for row in rows), 1.0, delta=0.02)
        self.assertTrue(all(row["valid_area_fraction"] is None for row in rows))
        self.assertTrue(all(row["activity_status"] in {"DETECTED_ACTIVITY", "UNKNOWN"} for row in rows))
        no_detection_rows = [row for row in rows if row["modis_count"] == 0 and row["viirs_count"] == 0]
        self.assertTrue(all(row["activity_status"] == "UNKNOWN" for row in no_detection_rows))
        self.assertIn("FIRMS_DETECTION_ONLY", rows[0]["opportunity_summary"])

    def test_large_bbox_daily_grid_persists_only_touched_cell_periods(self):
        # A 5x4 degree box at 1 km has roughly 224k grid slots. A dense
        # 31-day grid would approach 7 million rows even with one detection.
        self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0)
        self.run_qc()
        result = build_common_grid_summary(
            self.repository, bbox="73,26,78,30",
            start_date="2024-01-01", end_date="2024-01-31",
            grid_resolution_km=1.0, period_days=1,
        )
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["storage_strategy"], "SPARSE_DETECTED_CELL_PERIODS")
        self.assertGreater(result["grid_bbox_slots_upper_bound"], 200_000)
        self.assertLess(result["output_count"], 20)
        rows = self.repository.fetch_aggregation_results(result["processing_run_id"])
        self.assertEqual(len(rows), result["output_count"])
        self.assertTrue(all(row["activity_status"] == "DETECTED_ACTIVITY" for row in rows))
        self.assertTrue(all(row["valid_area_fraction"] is None for row in rows))
        run = self.repository.get_processing_run(result["processing_run_id"])
        self.assertEqual(run["module_name"], "AGGREGATION")
        self.assertEqual(run["status"], "COMPLETED")

    def test_grid_rejects_non_matching_processing_run_as_matching_input(self):
        qc = self.run_qc()
        with self.assertRaisesRegex(ValueError, "not 'MATCHING'"):
            build_common_grid_summary(
                self.repository, bbox="75.75,26.87,75.83,26.95",
                start_date="2024-01-02", end_date="2024-01-02",
                matching_run_id=qc["processing_run_id"],
            )

    def test_list_processing_runs_returns_grid_run_id_not_matching_input_id(self):
        self.add_observation("MODIS", lon=75.7873, lat=26.9124)
        self.run_qc()
        result = build_common_grid_summary(
            self.repository, bbox="75.75,26.87,75.83,26.95",
            start_date="2024-01-02", end_date="2024-01-02",
            grid_resolution_km=1.0,
        )
        runs = self.repository.list_processing_runs(module_name="AGGREGATION", limit=10)
        self.assertIn(result["processing_run_id"], [r["processing_run_id"] for r in runs])

    def test_geojson_exports_all_three_valid_feature_collections(self):
        self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0)
        self.add_observation("VIIRS", lon=75.7874, lat=26.9124, scan=0.375, track=0.375)
        self.run_qc()
        match = run_footprint_matching(
            self.repository, bbox="75.7,26.8,75.9,27.0",
            start_date="2024-01-02", end_date="2024-01-02",
        )
        export_dir = Path(self.tempdir.name) / "geojson"
        paths = export_validation_geojson(
            self.repository, processing_run_id=match["processing_run_id"], output_dir=export_dir
        )
        self.assertEqual(set(paths), {"footprints.geojson", "candidate_links.geojson", "common_grid.geojson"})
        documents = {name: json.loads(Path(path).read_text()) for name, path in paths.items()}
        self.assertEqual(documents["footprints.geojson"]["type"], "FeatureCollection")
        self.assertEqual(documents["candidate_links.geojson"]["type"], "FeatureCollection")
        self.assertEqual(documents["common_grid.geojson"]["type"], "FeatureCollection")
        self.assertGreaterEqual(len(documents["footprints.geojson"]["features"]), 2)
        self.assertGreaterEqual(len(documents["candidate_links.geojson"]["features"]), 1)
        self.assertEqual(documents["footprints.geojson"]["features"][0]["geometry"]["type"], "Polygon")

    def test_common_grid_run_exports_nonempty_grid_and_linked_candidate_geojson(self):
        self.add_observation("MODIS", lon=75.7873, lat=26.9124, scan=1.0, track=1.0)
        self.add_observation("VIIRS", lon=75.7874, lat=26.9124, scan=0.375, track=0.375)
        self.run_qc()
        matching = run_baseline_matching(
            self.repository, start_date="2024-01-02", end_date="2024-01-02",
            max_distance_km=2.0, max_time_difference_minutes=30.0,
        )
        result = build_common_grid_summary(
            self.repository, bbox="75.75,26.87,75.83,26.95",
            start_date="2024-01-02", end_date="2024-01-02", grid_resolution_km=1.0,
            matching_run_id=matching["processing_run_id"],
        )
        paths = export_validation_geojson(
            self.repository, processing_run_id=result["processing_run_id"],
            output_dir=Path(self.tempdir.name) / "grid-export",
        )
        grid_doc = json.loads(Path(paths["common_grid.geojson"]).read_text())
        links_doc = json.loads(Path(paths["candidate_links.geojson"]).read_text())
        self.assertGreater(len(grid_doc["features"]), 0)
        self.assertEqual(grid_doc["features"][0]["geometry"]["type"], "Polygon")
        self.assertGreaterEqual(len(links_doc["features"]), 1)
        grid_properties = grid_doc["features"][0]["properties"]
        self.assertIsInstance(grid_properties["frp_sum"], dict)
        self.assertEqual(set(grid_properties["frp_sum"]), {"MODIS_mw", "VIIRS_mw"})

    def test_new_module_tables_compile_for_postgresql(self):
        for table in (candidate_associations, measurement_results, aggregation_results):
            ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
            self.assertIn("IDENTITY", ddl)
            self.assertIn("REFERENCES", ddl)
        candidate_ddl = str(CreateTable(candidate_associations).compile(dialect=postgresql.dialect()))
        self.assertIn("AMBIGUOUS_GROUP", candidate_ddl)

    def test_matching_uses_only_qc_suitable_observations(self):
        valid_id = self.add_observation("MODIS", lon=75.78, lat=26.91)
        invalid_id = self.add_observation("VIIRS", lon=75.7801, lat=26.91)
        qc = self.run_qc()
        with self.repository.engine.begin() as connection:
            connection.execute(
                update(qc_results)
                .where(
                    qc_results.c.processing_run_id == qc["processing_run_id"],
                    qc_results.c.observation_id == invalid_id,
                )
                .values(suitability_for_matching=False, quality_status="INVALID")
            )
        result = run_baseline_matching(
            self.repository, start_date="2024-01-02", end_date="2024-01-02"
        )
        records = self.fetch_candidates(result["processing_run_id"])
        # The unsuitable row is not processed or emitted as an unmatched node.
        node_ids = {
            value for row in records
            for value in (row["modis_observation_id"], row["viirs_observation_id"])
            if value is not None
        }
        self.assertIn(valid_id, node_ids)
        self.assertNotIn(invalid_id, node_ids)

    def test_sqlite_old_processing_run_constraint_is_migrated_and_data_preserved(self):
        legacy_path = Path(self.tempdir.name) / "legacy.sqlite3"
        legacy_engine = create_engine(f"sqlite:///{legacy_path}")
        with legacy_engine.begin() as connection:
            connection.exec_driver_sql("""
                CREATE TABLE processing_runs (
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
                    CHECK (module_name IN ('QC','OPPORTUNITY'))
                )
            """)
            connection.exec_driver_sql("""
                INSERT INTO processing_runs (
                    processing_run_id, module_name, method_version, configuration_json,
                    started_at, input_count, output_count, status
                ) VALUES (7, 'QC', 'legacy', '{}', '2024-01-01 00:00:00', 1, 1, 'COMPLETED')
            """)
        legacy_engine.dispose()
        migrated = connect_db(legacy_path)
        try:
            run_id = migrated.create_processing_run("MATCHING", "1.0", "{}")
            self.assertGreater(run_id, 0)
            with migrated.engine.connect() as connection:
                old = connection.execute(
                    select(processing_runs).where(processing_runs.c.processing_run_id == 7)
                ).mappings().one()
            self.assertEqual(old["module_name"], "QC")
            self.assertEqual(old["method_version"], "legacy")
        finally:
            migrated.close()

    def test_module6_processing_run_names_are_accepted(self):
        for module_name in ("MATCHING", "MEASUREMENT", "AGGREGATION"):
            run_id = self.repository.create_processing_run(module_name, "test", "{}")
            self.assertGreater(run_id, 0)


if __name__ == "__main__":
    unittest.main()

    def test_midnight_boundary_padding_matches_utc_partner_and_assigns_primary_day(self):
        modis_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124)
        viirs_id = self.add_observation("VIIRS", lon=75.7874, lat=26.9124)
        with self.repository.engine.begin() as connection:
            connection.execute(update(__import__("db").observations).where(
                __import__("db").observations.c.observation_id == modis_id
            ).values(acquired_at=datetime(2026, 11, 12, 23, 55, tzinfo=timezone.utc)))
            connection.execute(update(__import__("db").observations).where(
                __import__("db").observations.c.observation_id == viirs_id
            ).values(acquired_at=datetime(2026, 11, 13, 0, 5, tzinfo=timezone.utc)))
        self.run_qc()
        result = run_baseline_matching(
            self.repository, start_date="2026-11-12", end_date="2026-11-12",
            bbox="75.7,26.8,75.9,27.0", window_preset=15,
        )
        self.assertEqual(result["candidate_edges"], 1)
        self.assertEqual(result["requested_window_observations"], 1)
        self.assertEqual(result["padded_fetch_observations"], 2)
        edge = next(row for row in self.fetch_candidates(result["processing_run_id"])
                    if row["modis_observation_id"] is not None and row["viirs_observation_id"] is not None)
        self.assertAlmostEqual(edge["time_difference_minutes"], 10.0)
        self.assertTrue(edge["is_cross_midnight"])
        self.assertEqual(edge["primary_calendar_day"], "2026-11-12")
        explanation = json.loads(edge["explanation_json"])
        self.assertIn("UTC midnight", explanation["plain_language"])

    def test_cross_midnight_candidate_group_is_counted_only_on_primary_utc_day(self):
        modis_id = self.add_observation("MODIS", lon=75.7873, lat=26.9124)
        viirs_id = self.add_observation("VIIRS", lon=75.7874, lat=26.9124)
        from db import observations
        with self.repository.engine.begin() as connection:
            connection.execute(update(observations).where(observations.c.observation_id == modis_id).values(
                acquired_at=datetime(2026, 11, 12, 23, 55, tzinfo=timezone.utc)))
            connection.execute(update(observations).where(observations.c.observation_id == viirs_id).values(
                acquired_at=datetime(2026, 11, 13, 0, 5, tzinfo=timezone.utc)))
        self.run_qc()
        matching = run_baseline_matching(
            self.repository, start_date="2026-11-12", end_date="2026-11-12",
            bbox="75.7,26.8,75.9,27.0", window_preset=15,
        )
        day_one = build_common_grid_summary(
            self.repository, bbox="75.7,26.8,75.9,27.0", start_date="2026-11-12",
            end_date="2026-11-12", matching_run_id=matching["processing_run_id"],
        )
        day_two = build_common_grid_summary(
            self.repository, bbox="75.7,26.8,75.9,27.0", start_date="2026-11-13",
            end_date="2026-11-13", matching_run_id=matching["processing_run_id"],
        )
        rows_one = self.repository.fetch_aggregation_results(day_one["processing_run_id"])
        rows_two = self.repository.fetch_aggregation_results(day_two["processing_run_id"])
        self.assertTrue(rows_one)
        self.assertTrue(any(row["is_cross_midnight"] for row in rows_one))
        self.assertTrue(all(row["primary_calendar_day"] == "2026-11-12" for row in rows_one))
        group_count_day_one = sum(row["matched_group_count"] or 0 for row in rows_one)
        group_count_day_two = sum(row["matched_group_count"] or 0 for row in rows_two)
        self.assertGreaterEqual(group_count_day_one, 1)
        self.assertEqual(group_count_day_two, 0)

    def test_platform_selection_filters_observations_and_stores_cache_key(self):
        self.add_observation("MODIS")
        self.add_observation("VIIRS")
        self.run_qc()
        result = run_baseline_matching(
            self.repository, start_date="2024-01-02", end_date="2024-01-02",
            bbox="75.7,26.8,75.9,27.0", platforms=["Terra"], window_preset=30,
            data_source_mode="FIRMS", grid_resolution_km=2.0,
        )
        self.assertEqual(result["input_observations"], 1)
        self.assertEqual(result["candidate_edges"], 0)
        run = self.repository.get_processing_run(result["processing_run_id"])
        self.assertEqual(run["run_cache_key"], result["run_cache_key"])
        config = json.loads(run["configuration_json"])
        self.assertEqual(config["selected_platforms"], ["Terra"])
        self.assertEqual(config["grid_resolution_km"], 2.0)
        self.assertEqual(config["max_time_difference_minutes"], 30.0)

    def test_temporal_preset_custom_and_diurnal_drift_warning(self):
        modis_id = self.add_observation("MODIS")
        viirs_id = self.add_observation("VIIRS")
        from db import observations
        with self.repository.engine.begin() as connection:
            connection.execute(update(observations).where(observations.c.observation_id == modis_id).values(
                acquired_at=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)))
            connection.execute(update(observations).where(observations.c.observation_id == viirs_id).values(
                acquired_at=datetime(2024, 1, 2, 13, 10, tzinfo=timezone.utc)))
        self.run_qc()
        narrow = run_baseline_matching(
            self.repository, start_date="2024-01-02", end_date="2024-01-02",
            bbox="75.7,26.8,75.9,27.0", window_preset=60,
        )
        self.assertEqual(narrow["candidate_edges"], 0)
        broad = run_baseline_matching(
            self.repository, start_date="2024-01-02", end_date="2024-01-02",
            bbox="75.7,26.8,75.9,27.0", temporal_window_minutes=90,
        )
        self.assertEqual(broad["candidate_edges"], 1)
        self.assertTrue(any(item["code"] == "DIURNAL_DRIFT_WARNING" for item in broad["warnings"]))
        edge = next(row for row in self.fetch_candidates(broad["processing_run_id"])
                    if row["modis_observation_id"] is not None and row["viirs_observation_id"] is not None)
        explanation = json.loads(edge["explanation_json"])
        self.assertTrue(any(item["code"] == "DIURNAL_DRIFT_WARNING" for item in explanation["warnings"]))
        with self.assertRaisesRegex(ValueError, "between 1 and 360"):
            run_baseline_matching(self.repository, temporal_window_minutes=361)

    def test_out_of_range_selected_platform_emits_diagnostic(self):
        result = run_baseline_matching(
            self.repository, start_date="2010-01-01", end_date="2010-01-02",
            bbox="75.7,26.8,75.9,27.0", platforms=["NOAA-21"], window_preset=15,
        )
        self.assertTrue(any(item["code"] == "PLATFORM_OUT_OF_OPERATIONAL_RANGE" for item in result["warnings"]))

