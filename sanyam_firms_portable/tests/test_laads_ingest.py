import tempfile
import unittest
import sys
import types
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

import sanyam_firms_portable.laads_ingest as li
from db import connect_db


class TestLAADSProductAndDecode(unittest.TestCase):
    def test_hdf4_reader_selects_sds_by_name_from_metadata_dictionary(self):
        arrays = {
            "fire mask": np.array([[5, 8]], dtype=np.uint8),
            "FP_latitude": np.array([27.0]),
        }
        class FakeDataset:
            def __init__(self, value):
                self.value = value
                self.closed = False
            def get(self):
                return self.value
            def endaccess(self):
                self.closed = True
        datasets = {name: FakeDataset(value) for name, value in arrays.items()}
        class FakeSD:
            def __init__(self, _path, _mode):
                self.closed = False
            def datasets(self):
                # pyhdf returns name -> (index, dimensions, type, attr_count)
                return {name: (i, value.shape, 0, 0) for i, (name, value) in enumerate(arrays.items())}
            def select(self, name):
                return datasets[name]
            def attributes(self):
                return {"CoreMetadata.0": "metadata"}
            def end(self):
                self.closed = True
        fake_pyhdf = types.ModuleType("pyhdf")
        fake_sd = types.ModuleType("pyhdf.SD")
        fake_sd.SD = FakeSD
        fake_sd.SDC = types.SimpleNamespace(READ=1)
        fake_pyhdf.SD = fake_sd
        with patch.dict(sys.modules, {"pyhdf": fake_pyhdf, "pyhdf.SD": fake_sd}):
            decoded, _attrs = li._read_hdf4(Path("mock.hdf"))
        self.assertEqual(set(decoded), set(arrays))
        np.testing.assert_array_equal(decoded["fire mask"], arrays["fire mask"])
        self.assertTrue(all(dataset.closed for dataset in datasets.values()))

    def test_hdf4_reader_treats_zero_length_fire_pixel_vectors_as_empty(self):
        class FakeDataset:
            def __init__(self, value=None, should_fail=False):
                self.value = value
                self.should_fail = should_fail
            def get(self):
                if self.should_fail:
                    raise ValueError("SDreaddata failure for zero-length SDS")
                return self.value
            def endaccess(self):
                return None

        fire_mask = np.array([[5, 5], [4, 5]], dtype=np.uint8)
        names = ["FP_line", "FP_sample", "FP_latitude", "FP_longitude", "FP_power"]
        datasets = {"fire mask": FakeDataset(fire_mask)}
        datasets.update({name: FakeDataset(should_fail=True) for name in names})

        class FakeSD:
            def __init__(self, _path, _mode):
                pass
            def datasets(self):
                result = {"fire mask": (0, (2, 2), 0, 0)}
                result.update({name: (i + 1, (0,), 5, 0) for i, name in enumerate(names)})
                return result
            def select(self, name):
                return datasets[name]
            def attributes(self):
                return {"FirePix": 0}
            def end(self):
                return None

        fake_pyhdf = types.ModuleType("pyhdf")
        fake_sd = types.ModuleType("pyhdf.SD")
        fake_sd.SD = FakeSD
        fake_sd.SDC = types.SimpleNamespace(READ=1, INT16=22, FLOAT32=5)
        fake_pyhdf.SD = fake_sd
        with patch.dict(sys.modules, {"pyhdf": fake_pyhdf, "pyhdf.SD": fake_sd}):
            arrays, attrs = li._read_hdf4(Path("empty-fire-table.hdf"))
        self.assertEqual(arrays["FP_line"].shape, (0,))
        self.assertEqual(arrays["FP_latitude"].shape, (0,))
        self.assertEqual(set(attrs["_EMPTY_SDS_NAMES"]), set(names))
        self.assertNotIn("_SDS_READ_WARNINGS", attrs)
        rows, _mask = li.extract_modis_granule(
            arrays, attrs, product="MOD14",
            granule=li.parse_granule_filename("MOD14.A2024001.0000.061.2024001000000.hdf"),
        )
        self.assertEqual(rows, [])

    def test_hdf4_reader_does_not_hide_unreadable_nonempty_fire_vectors(self):
        class FakeDataset:
            def get(self):
                raise ValueError("SDreaddata failure")
            def endaccess(self):
                return None
        class FakeSD:
            def __init__(self, _path, _mode):
                pass
            def datasets(self):
                return {
                    "fire mask": (0, (2, 2), 21, 0),
                    "FP_line": (1, (3,), 22, 0),
                }
            def select(self, _name):
                return FakeDataset()
            def attributes(self):
                return {"FirePix": 3}
            def end(self):
                return None
        fake_pyhdf = types.ModuleType("pyhdf")
        fake_sd = types.ModuleType("pyhdf.SD")
        fake_sd.SD = FakeSD
        fake_sd.SDC = types.SimpleNamespace(READ=1)
        fake_pyhdf.SD = fake_sd
        with patch.dict(sys.modules, {"pyhdf": fake_pyhdf, "pyhdf.SD": fake_sd}):
            with self.assertRaisesRegex(li.LAADSError, "Required MODIS fire-pixel SDS 'FP_line'"):
                li._read_hdf4(Path("bad-nonempty-vector.hdf"))

    def test_hdf4_reader_skips_unreadable_optional_sds_and_records_warning(self):
        arrays = {
            "fire mask": np.array([[5, 8]], dtype=np.uint8),
            "FP_latitude": np.array([27.0]),
        }
        class FakeDataset:
            def __init__(self, value=None, error=None):
                self.value = value
                self.error = error
            def get(self):
                if self.error:
                    raise ValueError(self.error)
                return self.value
            def endaccess(self):
                return None
        datasets = {
            "fire mask": FakeDataset(arrays["fire mask"]),
            "FP_latitude": FakeDataset(arrays["FP_latitude"]),
            "optional diagnostic layer": FakeDataset(error="SDreaddata failure"),
        }
        class FakeSD:
            def __init__(self, _path, _mode):
                pass
            def datasets(self):
                return {name: (i, (), 0, 0) for i, name in enumerate(datasets)}
            def select(self, name):
                return datasets[name]
            def attributes(self):
                return {"CoreMetadata.0": "metadata"}
            def end(self):
                return None
        fake_pyhdf = types.ModuleType("pyhdf")
        fake_sd = types.ModuleType("pyhdf.SD")
        fake_sd.SD = FakeSD
        fake_sd.SDC = types.SimpleNamespace(READ=1)
        fake_pyhdf.SD = fake_sd
        with patch.dict(sys.modules, {"pyhdf": fake_pyhdf, "pyhdf.SD": fake_sd}):
            decoded, attrs = li._read_hdf4(Path("mock.hdf"))
        self.assertIn("fire mask", decoded)
        self.assertNotIn("optional diagnostic layer", decoded)
        self.assertEqual(attrs["_SDS_READ_WARNINGS"][0]["code"], "HDF4_SDS_READ_FAILED")
        self.assertEqual(attrs["_SDS_READ_WARNINGS"][0]["dataset"], "optional diagnostic layer")

    def test_hdf4_reader_rejects_unreadable_required_fire_mask(self):
        class FakeDataset:
            def get(self):
                raise ValueError("SDreaddata failure")
            def endaccess(self):
                return None
        class FakeSD:
            def __init__(self, _path, _mode):
                pass
            def datasets(self):
                return {"fire mask": (0, (2, 2), 0, 0)}
            def select(self, _name):
                return FakeDataset()
            def attributes(self):
                return {}
            def end(self):
                return None
        fake_pyhdf = types.ModuleType("pyhdf")
        fake_sd = types.ModuleType("pyhdf.SD")
        fake_sd.SD = FakeSD
        fake_sd.SDC = types.SimpleNamespace(READ=1)
        fake_pyhdf.SD = fake_sd
        with patch.dict(sys.modules, {"pyhdf": fake_pyhdf, "pyhdf.SD": fake_sd}):
            with self.assertRaisesRegex(li.LAADSError, "Required MODIS HDF4 'fire mask' SDS could not be read"):
                li._read_hdf4(Path("broken.hdf"))

    def test_product_catalog_has_expected_products_and_collections(self):
        self.assertEqual(set(li.PRODUCTS), {"MOD14", "MYD14", "VNP14IMG", "VJ114IMG", "VJ214IMG"})
        self.assertEqual(li.PRODUCTS["MOD14"]["collection"], "61")
        self.assertEqual(li.PRODUCTS["MYD14"]["collection"], "61")
        self.assertEqual(li.PRODUCTS["MOD14"]["archive_set"], "61")
        self.assertEqual(li.PRODUCTS["MYD14"]["archive_set"], "61")
        for product in ("VNP14IMG", "VJ114IMG", "VJ214IMG"):
            self.assertEqual(li.PRODUCTS[product]["collection"], "002")
            self.assertEqual(li.PRODUCTS[product]["archive_set"], "5200")

    def test_parse_granule_filename_utc_and_collection(self):
        parsed = li.parse_granule_filename("MOD14.A2026320.1230.061.2026320123456.hdf")
        self.assertEqual(parsed.product_id, "MOD14")
        self.assertEqual(parsed.collection, "061")
        self.assertEqual(parsed.start_at, datetime(2026, 11, 16, 12, 30, tzinfo=timezone.utc))

    def test_modis_mask_decode_classes_and_qa_flags(self):
        mask = np.array([[0, 3, 4, 5], [6, 7, 8, 9]], dtype=np.uint8)
        qa = np.full_like(mask, 0b10, dtype=np.uint32)  # bit 0-1: land
        qa[1, 1] |= (1 << 20) | (1 << 22)
        decoded = li.decode_modis_mask(mask, qa)
        self.assertTrue(decoded["not_processed"][0, 0])
        self.assertTrue(decoded["water"][0, 1])
        self.assertTrue(decoded["cloud"][0, 2])
        self.assertTrue(decoded["clear_land"][0, 3])
        self.assertTrue(decoded["low_confidence_fire"][1, 1])
        self.assertTrue(decoded["nominal_confidence_fire"][1, 2])
        self.assertTrue(decoded["high_confidence_fire"][1, 3])
        self.assertTrue(decoded["adjacent_cloud"][1, 1])
        self.assertTrue(decoded["sun_glint"][1, 1])

    def test_modis_land_water_requires_algorithm_qa_and_distinguishes_coast(self):
        mask = np.array([[5, 5, 5, 8]], dtype=np.uint8)
        qa = np.array([[0b10, 0b00, 0b01, 0b10]], dtype=np.uint32)
        decoded = li.decode_modis_mask(mask, qa)
        self.assertTrue(decoded["clear_land"][0, 0])
        self.assertTrue(decoded["water_by_qa"][0, 1])
        self.assertTrue(decoded["coast_by_qa"][0, 2])
        self.assertTrue(decoded["clear_land"][0, 1])  # native class 5 is non-fire land
        without_qa = li.decode_modis_mask(mask)
        self.assertTrue(without_qa["clear_land"][0, 0])

    def test_viirs_mask_decode_bowtie_sun_glint_water_cloud_and_confidence(self):
        mask = np.array([[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]], dtype=np.uint8)
        qa = np.zeros_like(mask, dtype=np.uint32)
        qa[0, 1] = 1 << 22
        qa[0, 2] = 1 << 17
        decoded = li.decode_viirs_mask(mask, qa)
        self.assertTrue(decoded["not_processed"][0, 0])
        self.assertTrue(decoded["bowtie_deletion"][0, 1])
        self.assertTrue(decoded["sun_glint"][0, 2])
        self.assertTrue(decoded["water"][0, 3])
        self.assertTrue(decoded["cloud"][0, 4])
        self.assertTrue(decoded["clear_land"][1, 0])
        self.assertTrue(decoded["low_confidence_fire"][1, 2])
        self.assertTrue(decoded["nominal_confidence_fire"][1, 3])
        self.assertTrue(decoded["high_confidence_fire"][1, 4])
        self.assertTrue(decoded["bowtie_residual_qa"][0, 1])
        self.assertTrue(decoded["sun_glint_qa"][0, 2])

    def test_modis_array_adapter_retains_fire_mask_and_sparse_pixel_attributes(self):
        granule = li.parse_granule_filename("MOD14.A2024001.0000.061.2024001000000.hdf")
        arrays = {
            "fire mask": np.array([[8, 5], [4, 9]], dtype=np.uint8),
            "algorithm QA": np.zeros((2, 2), dtype=np.uint32),
            "FP_latitude": np.array([26.9, 27.0]),
            "FP_longitude": np.array([75.8, 75.9]),
            "FP_power": np.array([12.5, 21.0]),
            "FP_T21": np.array([320.0, 330.0]),
            "FP_T31": np.array([300.0, 310.0]),
            "FP_confidence": np.array([80, 95]),
            "FP_line": np.array([0, 1]),
            "FP_sample": np.array([0, 1]),
        }
        rows, mask_data = li.extract_modis_granule(arrays, {"FirePix": 2}, product="MOD14", granule=granule)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["fire_mask_class"], 8)
        self.assertEqual(rows[1]["fire_mask_class"], 9)
        self.assertEqual(float(rows[0]["fields"]["FP_power"]), 12.5)
        self.assertEqual(mask_data["fire_mask"].shape, (2, 2))
        self.assertEqual(mask_data["decoded_mask"]["fire"].sum(), 2)

    def test_modis_adapter_rejects_missing_required_geolocation_vector(self):
        granule = li.parse_granule_filename("MOD14.A2024001.0000.061.2024001000000.hdf")
        arrays = {
            "fire mask": np.array([[8]], dtype=np.uint8),
            "FP_latitude": np.array([26.9]),
            "FP_line": np.array([0]),
            "FP_sample": np.array([0]),
            # FP_longitude intentionally absent: a detection cannot be located.
        }
        with self.assertRaisesRegex(li.LAADSError, "FP_longitude"):
            li.extract_modis_granule(arrays, {"FirePix": 1}, product="MOD14", granule=granule)

    def test_viirs_ingest_mode_skips_full_decoded_boolean_mask_layers(self):
        granule = li.parse_granule_filename("VNP14IMG.A2024001.0000.002.2024001000000.nc")
        arrays = {
            "fire mask": np.array([[7, 8], [9, 5]], dtype=np.uint8),
            "algorithm QA": np.zeros((2, 2), dtype=np.uint32),
            "FP_latitude": np.array([26.9, 27.0, 27.1]),
            "FP_longitude": np.array([75.8, 75.9, 76.0]),
            "FP_power": np.array([8.0, 12.0, 18.0]),
            "FP_line": np.array([0, 0, 1]),
            "FP_sample": np.array([0, 1, 0]),
        }
        rows, mask_data = li.extract_viirs_granule(
            arrays, {"FirePix": 3}, product="VNP14IMG", granule=granule,
            decode_full_mask=False,
        )
        self.assertEqual(len(rows), 3)
        self.assertIsNone(mask_data["decoded_mask"])
        np.testing.assert_array_equal(mask_data["fire_mask"], arrays["fire mask"])

    def test_netcdf_sparse_reader_reads_each_variable_once_and_skips_unneeded_layers(self):
        class FakeVariable:
            def __init__(self, value):
                self.value = value
                self.reads = 0
                self.dtype = value.dtype
            def __getitem__(self, key):
                self.reads += 1
                return self.value

        class FakeDataset:
            def __init__(self):
                self.variables = {
                    "FireMask": FakeVariable(np.array([[5, 8]], dtype=np.uint8)),
                    "Algorithm_QA": FakeVariable(np.zeros((1, 2), dtype=np.uint32)),
                    "FP_line": FakeVariable(np.array([0], dtype=np.int16)),
                    "FP_sample": FakeVariable(np.array([1], dtype=np.int16)),
                    "FP_latitude": FakeVariable(np.array([27.0], dtype=np.float32)),
                    "FP_longitude": FakeVariable(np.array([76.0], dtype=np.float32)),
                    "FP_power": FakeVariable(np.array([10.0], dtype=np.float32)),
                    "UnneededLargeTemperatureLayer": FakeVariable(np.ones((100, 100), dtype=np.float32)),
                }
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def ncattrs(self):
                return []
            def getncattr(self, name):
                raise KeyError(name)

        fake_dataset = FakeDataset()
        fake_netcdf4 = types.ModuleType("netCDF4")
        fake_netcdf4.Dataset = lambda *_args, **_kwargs: fake_dataset
        with patch.dict(sys.modules, {"netCDF4": fake_netcdf4}):
            arrays, _attrs = li._read_hdf5(Path("mock.nc"), sparse_ingest=True)
        self.assertIn("FireMask", arrays)
        self.assertIn("FP_latitude", arrays)
        self.assertNotIn("UnneededLargeTemperatureLayer", arrays)
        self.assertEqual(fake_dataset.variables["FireMask"].reads, 1)
        self.assertEqual(fake_dataset.variables["FP_latitude"].reads, 1)
        self.assertEqual(fake_dataset.variables["UnneededLargeTemperatureLayer"].reads, 0)

    def test_viirs_array_adapter_retains_full_mask_and_fire_pixel_attributes(self):
        granule = li.parse_granule_filename("VNP14IMG.A2024001.0000.002.2024001000000.nc")
        arrays = {
            "fire mask": np.array([[7, 8], [9, 5]], dtype=np.uint8),
            "algorithm QA": np.zeros((2, 2), dtype=np.uint32),
            "FP_latitude": np.array([26.9, 27.0, 27.1]),
            "FP_longitude": np.array([75.8, 75.9, 76.0]),
            "FP_power": np.array([8.0, 12.0, 18.0]),
            "FP_T4": np.array([310.0, 320.0, 330.0]),
            "FP_T5": np.array([290.0, 300.0, 310.0]),
            "FP_MeanT4": np.array([305.0, 315.0, 325.0]),
            "FP_MeanT5": np.array([285.0, 295.0, 305.0]),
            "FP_AdjCloud": np.array([0, 1, 0]),
            "FP_confidence": np.array(["l", "n", "h"]),
            "FP_line": np.array([0, 0, 1]),
            "FP_sample": np.array([0, 1, 0]),
        }
        rows, mask_data = li.extract_viirs_granule(arrays, {"FirePix": 3}, product="VNP14IMG", granule=granule)
        self.assertEqual([row["fire_mask_class"] for row in rows], [7, 8, 9])
        self.assertEqual(float(rows[1]["fields"]["FP_MeanT4"]), 315.0)
        self.assertEqual(mask_data["fire_mask"].shape, (2, 2))
        self.assertEqual(mask_data["decoded_mask"]["fire"].sum(), 3)

    def test_viirs_adapter_rejects_missing_required_geolocation_vector(self):
        granule = li.parse_granule_filename("VNP14IMG.A2024001.0000.002.2024001000000.nc")
        arrays = {
            "fire mask": np.array([[8]], dtype=np.uint8),
            "FP_latitude": np.array([26.9]),
            "FP_longitude": np.array([75.8]),
            "FP_line": np.array([0]),
            # FP_sample intentionally absent.
        }
        with self.assertRaisesRegex(li.LAADSError, "FP_sample"):
            li.extract_viirs_granule(arrays, {"FirePix": 1}, product="VNP14IMG", granule=granule)

    def test_level2_line_interpolation_uses_zero_based_scanline_indices(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc)
        first, first_precision = li._time_for_pixel(start, end, 0, 2)
        last, last_precision = li._time_for_pixel(start, end, 1, 2)
        self.assertEqual(first, start)
        self.assertEqual(last, end)
        self.assertEqual(first_precision, "LINEAR_INTERPOLATED_FROM_GRANULE_TIME_BOUNDS")
        self.assertEqual(last_precision, "LINEAR_INTERPOLATED_FROM_GRANULE_TIME_BOUNDS")

    def test_true_four_corner_pixel_polygon_is_only_emitted_from_corner_grid(self):
        arrays = {
            "latitude_corners": np.array([[[10.0, 10.0, 9.0, 9.0]]]),
            "longitude_corners": np.array([[[20.0, 21.0, 21.0, 20.0]]]),
        }
        wkt = li._pixel_footprint_wkt(arrays, 0, 0)
        self.assertIsNotNone(wkt)
        from shapely import wkt as shapely_wkt
        polygon = shapely_wkt.loads(wkt)
        self.assertEqual(polygon.geom_type, "Polygon")
        self.assertAlmostEqual(polygon.area, 1.0)
        self.assertIsNone(li._pixel_footprint_wkt({"latitude": np.array([[10.0]]), "longitude": np.array([[20.0]])}, 0, 0))

    def test_platform_availability_warnings_cover_noaa21_and_snpp_cutoff(self):
        noaa21 = li.platform_availability_diagnostics(
            ["NOAA-21"], datetime(2022, 1, 1, tzinfo=timezone.utc), datetime(2022, 2, 1, tzinfo=timezone.utc)
        )
        self.assertEqual(noaa21[0]["code"], "PLATFORM_OUT_OF_OPERATIONAL_RANGE")
        snpp = li.platform_availability_diagnostics(
            ["Suomi-NPP"], datetime(2026, 11, 3, tzinfo=timezone.utc), datetime(2026, 11, 4, tzinfo=timezone.utc)
        )
        self.assertEqual(snpp[0]["code"], "PLATFORM_OUT_OF_OPERATIONAL_RANGE")


class TestLAADSClient(unittest.TestCase):
    def test_search_granules_uses_authenticated_api_v2_query(self):
        class FakeResponse:
            status_code = 200
            text = '<a href="/api/v2/content/archives/allData/61/MOD14/2024/001/MOD14.A2024001.0000.061.2024001000000.hdf">granule</a>'
            def raise_for_status(self):
                return None
        class FakeSession:
            def __init__(self):
                self.headers = {}
                self.calls = []
            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return FakeResponse()
        session = FakeSession()
        client = li.LAADSClient("example-download-token", session=session)
        urls = client.search_granules(
            product="MOD14", collection="61", start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2), bbox=[73, 26, 78, 30],
        )
        self.assertEqual(len(urls), 1)
        self.assertIn("MOD14.A2024001", urls[0])
        self.assertEqual(session.headers["Authorization"], "Bearer example-download-token")
        params = session.calls[0][1]["params"]
        self.assertEqual(params["products"], "MOD14")
        self.assertEqual(params["archiveSets"], "61")
        self.assertNotIn("collections", params)
        self.assertEqual(params["temporalRanges"], "2024-01-01..2024-01-02")
        self.assertIn("[BBOX]", params["regions"])

    def test_viirs_search_uses_archive_set_5200_not_filename_collection_002(self):
        class FakeResponse:
            status_code = 200
            text = ""
            def json(self):
                return {"items": []}
        class FakeSession:
            def __init__(self):
                self.headers = {}
                self.calls = []
            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return FakeResponse()
        session = FakeSession()
        client = li.LAADSClient("example-download-token", session=session)
        client.search_granules(
            product="VNP14IMG", collection="002", start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5), bbox=[73, 26, 78, 30],
        )
        params = session.calls[0][1]["params"]
        self.assertEqual(params["archiveSets"], "5200")
        self.assertEqual(params["products"], "VNP14IMG")
        self.assertNotIn("collections", params)

    def test_download_streams_to_part_file_then_atomically_renames(self):
        class FakeResponse:
            status_code = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def iter_content(self, chunk_size):
                yield b"part-a"
                yield b"part-b"
        class FakeSession:
            headers = {}
            def get(self, url, **kwargs):
                self.url = url
                return FakeResponse()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "granule.nc"
            session = FakeSession()
            client = li.LAADSClient("token", session=session)
            returned = client.download("/api/v2/content/archives/test.nc", target)
            self.assertEqual(returned, target)
            self.assertEqual(target.read_bytes(), b"part-apart-b")
            self.assertFalse(target.with_suffix(".nc.part").exists())
            self.assertIn("ladsweb.modaps.eosdis.nasa.gov", session.url)

    def test_download_refuses_external_host(self):
        client = li.LAADSClient("token")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                client.download("https://attacker.example/file.hdf", Path(directory) / "x.hdf")


class TestLAADSIngestionPersistence(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.repository = connect_db(self.root / "sanyam.sqlite3")

    def tearDown(self):
        self.repository.close()
        self.tempdir.cleanup()

    def test_search_failure_for_one_product_does_not_abort_remaining_products(self):
        class FakeClient:
            def __init__(self):
                self.searched = []
            def search_granules(self, **kwargs):
                self.searched.append(kwargs["product"])
                if kwargs["product"] == "MOD14":
                    raise li.LAADSError("LAADS search failed HTTP 400: bad archive filter")
                return []
        client = FakeClient()
        result = li.ingest_laads(
            self.repository, client=client, products=["MOD14", "MYD14"],
            start_date=date(2024, 1, 1), end_date=date(2024, 1, 1),
            bbox=[73, 26, 78, 30], raw_root=self.root / "data" / "raw",
        )
        self.assertEqual(client.searched, ["MOD14", "MYD14"])
        self.assertEqual(result["products"]["MOD14"]["status"], "FAILED")
        self.assertEqual(result["products"]["MYD14"]["status"], "COMPLETED")
        self.assertEqual(result["products"]["MYD14"]["granules_discovered"], 0)
        self.assertEqual(result["warnings"][0]["code"], "LAADS_PRODUCT_SEARCH_FAILED")

    def test_ingestion_writes_immutable_raw_asset_and_observation_to_shared_schema(self):
        filename = "MOD14.A2024001.0000.061.2024001000000.hdf"
        granule = li.parse_granule_filename(filename)
        arrays = {
            "fire mask": np.array([[8]], dtype=np.uint8),
            "algorithm QA": np.zeros((1, 1), dtype=np.uint32),
        }
        field_values = {"FP_latitude": 26.9, "FP_longitude": 75.8, "FP_power": 11.0,
                        "FP_T21": 320.0, "FP_T31": 300.0, "FP_confidence": 85,
                        "FP_line": 1, "FP_sample": 1}
        extracted_row = {"index": 0, "line": 1, "sample": 1, "latitude": 26.9,
                         "longitude": 75.8, "fields": field_values, "fire_mask_class": 8, "algorithm_qa": 0}
        class FakeClient:
            def search_granules(self, **kwargs):
                return ["https://example.test/" + filename]
            def download(self, url, destination):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(b"immutable mocked HDF4 bytes")
        with patch.object(li, "_extract_file", return_value=(
            [extracted_row], {"fire_mask": arrays["fire mask"], "algorithm_qa": arrays["algorithm QA"]},
            {"StartTime": "2024-01-01T00:00:00Z", "EndTime": "2024-01-01T00:05:00Z", "_arrays": arrays},
        )):
            result = li.ingest_laads(
                self.repository, client=FakeClient(), products=["MOD14"],
                start_date=date(2024, 1, 1), end_date=date(2024, 1, 1),
                bbox=[73, 26, 78, 30], raw_root=self.root / "data" / "raw",
            )
        self.assertEqual(result["products"]["MOD14"]["status"], "COMPLETED")
        assets = self.repository.fetch_raw_assets(platforms=["Terra"])
        self.assertEqual(len(assets), 1)
        asset = assets[0]
        self.assertEqual(asset["product_id"], "MOD14")
        self.assertEqual(asset["collection_id"], "61")
        self.assertEqual(asset["platform"], "Terra")
        self.assertTrue(asset["sha256"])
        self.assertEqual(Path(asset["storage_uri"]).read_bytes(), b"immutable mocked HDF4 bytes")
        self.assertIn("/modis/terra/", asset["storage_uri"].replace("\\", "/"))
        from sqlalchemy import select
        from db import observations
        with self.repository.engine.connect() as connection:
            rows = connection.execute(select(observations)).mappings().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["raw_asset_id"], asset["raw_asset_id"])
        self.assertEqual(rows[0]["fire_mask_class"], 8)


if __name__ == "__main__":
    unittest.main()
