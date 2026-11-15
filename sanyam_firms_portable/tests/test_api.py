import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from db import connect_db
from api import create_app


class TestSANYAMAPI(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tempdir.name) / "api.sqlite3"
        self.client = TestClient(create_app(lambda _url=None: connect_db(db_path)))

    def tearDown(self):
        self.client.close()
        self.tempdir.cleanup()

    def test_health_and_platform_catalog(self):
        health = self.client.get("/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        catalog = self.client.get("/platforms").json()
        self.assertEqual({p["platform"] for p in catalog["platforms"]},
                         {"Terra", "Aqua", "Suomi-NPP", "NOAA-20", "NOAA-21"})
        self.assertEqual(catalog["temporal_window_presets_minutes"], [5, 15, 30, 60, 120])

    def test_platform_diagnostics_endpoint(self):
        response = self.client.get("/diagnostics/platforms", params=[
            ("platforms", "NOAA-21"), ("start_date", "2022-01-01"), ("end_date", "2022-01-02")
        ])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["diagnostics"][0]["code"], "PLATFORM_OUT_OF_OPERATIONAL_RANGE")

    def test_matching_endpoint_accepts_custom_window_and_returns_cache_key(self):
        response = self.client.post("/matching/run", json={
            "method": "BASELINE_HAVERSINE",
            "start_date": "2024-01-01", "end_date": "2024-01-02",
            "bbox": [73, 26, 78, 30],
            "sensors": ["MODIS", "VIIRS"],
            "platforms": ["Terra", "NOAA-20"],
            "custom_window_minutes": 90,
            "data_source_mode": "BOTH",
        })
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["max_time_difference_minutes"], 90.0)
        self.assertTrue(payload["run_cache_key"])
        self.assertTrue(any(w["code"] == "DIURNAL_DRIFT_WARNING" for w in payload["warnings"]))

    def test_matching_rejects_single_sensor_request(self):
        response = self.client.post("/matching/run", json={
            "method": "BASELINE_HAVERSINE",
            "start_date": "2024-01-01", "end_date": "2024-01-02",
            "bbox": [73, 26, 78, 30], "sensors": ["VIIRS"],
        })
        self.assertEqual(response.status_code, 422)

    def test_qc_and_frp_measurement_run_interfaces(self):
        qc = self.client.post("/qc/run", json={"batch_size": 50})
        self.assertEqual(qc.status_code, 200, qc.text)
        qc_id = qc.json()["processing_run_id"]
        qc_rows = self.client.get(f"/qc/{qc_id}")
        self.assertEqual(qc_rows.status_code, 200, qc_rows.text)
        self.assertEqual(qc_rows.json()["count"], 0)

        frp = self.client.post("/measurement/frp/run", json={
            "start_date": "2024-01-01", "end_date": "2024-01-02",
            "bbox": [73, 26, 78, 30],
        })
        self.assertEqual(frp.status_code, 200, frp.text)
        frp_id = frp.json()["processing_run_id"]
        frp_rows = self.client.get(f"/measurement/{frp_id}")
        self.assertEqual(frp_rows.status_code, 200, frp_rows.text)
        self.assertEqual(frp_rows.json()["count"], 0)

    def test_matching_requires_both_sensor_families_in_platform_selection(self):
        response = self.client.post("/matching/run", json={
            "start_date": "2024-01-01", "end_date": "2024-01-02",
            "bbox": [73, 26, 78, 30], "platforms": ["Terra"],
        })
        self.assertEqual(response.status_code, 422)

    def test_opportunity_endpoint_rejects_platforms_from_other_sensor_family(self):
        response = self.client.post("/opportunities/run", json={
            "sensor": "MODIS",
            "bbox": [73, 26, 78, 30],
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "platforms": ["NOAA-20"],
            "evidence_mode": "LEVEL2",
        })
        self.assertEqual(response.status_code, 422)

    def test_laads_ingest_endpoint_does_not_accept_or_echo_credentials(self):
        response = self.client.post("/ingestion/laads/run", json={
            "products": ["MOD14"], "start_date": "2024-01-01", "end_date": "2024-01-01",
            "bbox": [73, 26, 78, 30],
        })
        # Test environment intentionally has no EARTHDATA_TOKEN.
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "EARTHDATA_TOKEN is not configured on the server")


if __name__ == "__main__":
    unittest.main()
