#!/usr/bin/env python3
"""SANYAM LAADS DAAC Level-2 active-fire ingestion.

Supports MOD14/MYD14 HDF4 (pyhdf) and VNP14IMG/VJ114IMG/VJ214IMG
NetCDF4/HDF5 granules. Raw granules are stored unchanged; decoded values are
additional, traceable observations. No credential is embedded in source code.

Important temporal note: the published fire-pixel tables provide scan-line
indices and granule-level time bounds, not a per-fire-pixel UTC timestamp SDS.
When both bounds and FP_line exist this adapter linearly interpolates within the
granule and records that precision explicitly; it does not label that value as
an independently measured exact pixel timestamp.
"""
from __future__ import annotations

import argparse
import hashlib
import gc
import html.parser
import json
import logging
import math
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urljoin, urlparse

import numpy as np
import requests

from db import DatabaseRepository, connect_db

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - environment-only mode
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

LOG = logging.getLogger("sanyam.laads_ingest")
LAADS_BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov"
LAADS_API_BASE = f"{LAADS_BASE_URL}/api/v2"
PRODUCTS: dict[str, dict[str, Any]] = {
    "MOD14": {"sensor": "MODIS", "platform": "Terra", "collection": "61", "archive_set": "61", "ext": ".hdf", "instrument": "MODIS"},
    "MYD14": {"sensor": "MODIS", "platform": "Aqua", "collection": "61", "archive_set": "61", "ext": ".hdf", "instrument": "MODIS"},
    "VNP14IMG": {"sensor": "VIIRS", "platform": "Suomi-NPP", "collection": "002", "archive_set": "5200", "ext": ".nc", "instrument": "VIIRS"},
    "VJ114IMG": {"sensor": "VIIRS", "platform": "NOAA-20", "collection": "002", "archive_set": "5200", "ext": ".nc", "instrument": "VIIRS"},
    "VJ214IMG": {"sensor": "VIIRS", "platform": "NOAA-21", "collection": "002", "archive_set": "5200", "ext": ".nc", "instrument": "VIIRS"},
}
PLATFORM_ALIASES = {
    "TERRA": "Terra", "MOD": "Terra", "AQUA": "Aqua", "MYD": "Aqua",
    "SUOMI-NPP": "Suomi-NPP", "SNPP": "Suomi-NPP", "NPP": "Suomi-NPP",
    "NOAA-20": "NOAA-20", "NOAA20": "NOAA-20", "JPSS-1": "NOAA-20",
    "NOAA-21": "NOAA-21", "NOAA21": "NOAA-21", "JPSS-2": "NOAA-21",
}
# Operational periods are warnings/filters, not proof that a particular product
# file exists. Archive availability must still be checked with LAADS.
PLATFORM_OPERATIONAL: dict[str, tuple[datetime, datetime | None]] = {
    "Terra": (datetime(2000, 2, 24, tzinfo=timezone.utc), None),
    "Aqua": (datetime(2002, 7, 4, tzinfo=timezone.utc), None),
    "Suomi-NPP": (datetime(2012, 1, 19, tzinfo=timezone.utc), datetime(2026, 11, 2, 13, 0, tzinfo=timezone.utc)),
    "NOAA-20": (datetime(2018, 1, 1, tzinfo=timezone.utc), None),
    "NOAA-21": (datetime(2022, 11, 1, tzinfo=timezone.utc), None),
}


class LAADSError(RuntimeError):
    """LAADS discovery, download or decoder failure."""


@dataclass(frozen=True)
class GranuleName:
    product_id: str
    granule_id: str
    start_at: datetime
    collection: str


def canonical_platform(value: str) -> str:
    key = str(value).strip().upper()
    try:
        return PLATFORM_ALIASES[key]
    except KeyError as exc:
        raise ValueError(f"Unknown platform: {value}") from exc


def platform_availability_diagnostics(
    platforms: Sequence[str], start_at: datetime, end_at: datetime,
) -> list[dict[str, str]]:
    """Explain requested dates wholly or partly outside platform operations."""
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ValueError("platform availability windows must be timezone-aware")
    start_at, end_at = start_at.astimezone(timezone.utc), end_at.astimezone(timezone.utc)
    diagnostics: list[dict[str, str]] = []
    for requested in platforms:
        platform = canonical_platform(requested)
        active_start, active_end = PLATFORM_OPERATIONAL[platform]
        if end_at <= active_start or (active_end is not None and start_at >= active_end):
            if active_end is not None and start_at >= active_end:
                detail = f"{platform} science-product availability ended at {active_end.isoformat()}"
            else:
                detail = f"{platform} science-product availability starts at {active_start.isoformat()}"
            diagnostics.append({"code": "PLATFORM_OUT_OF_OPERATIONAL_RANGE", "platform": platform, "message": detail})
        elif start_at < active_start or (active_end is not None and end_at > active_end):
            diagnostics.append({
                "code": "PLATFORM_PARTIALLY_OUT_OF_OPERATIONAL_RANGE", "platform": platform,
                "message": f"Requested interval partly lies outside the known operational interval for {platform}",
            })
    return diagnostics


def _utc_iso(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray):
        if value.size == 1:
            value = value.reshape(-1)[0]
        else:
            return None
    if isinstance(value, np.generic):
        value = value.item()
    text_value = str(value).strip().strip("\x00")
    if not text_value:
        return None
    # Common product metadata forms include ISO and YYYY-MM-DD / HH:MM:SS.
    try:
        result = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _metadata_datetime(attrs: Mapping[str, Any], prefix: str) -> datetime | None:
    candidates = [
        attrs.get(prefix),
        " ".join(str(attrs.get(k, "")).strip() for k in (f"{prefix}Date", f"{prefix}Time")).strip(),
    ]
    # Common NASA ECS/NetCDF spellings.
    if prefix.lower() == "start":
        candidates.extend([
            " ".join(str(attrs.get(k, "")).strip() for k in ("RangeBeginningDate", "RangeBeginningTime")).strip(),
            attrs.get("StartTime"), attrs.get("RangeBeginningTime"),
        ])
    else:
        candidates.extend([
            " ".join(str(attrs.get(k, "")).strip() for k in ("RangeEndingDate", "RangeEndingTime")).strip(),
            attrs.get("EndTime"), attrs.get("RangeEndingTime"),
        ])
    for candidate in candidates:
        parsed = _utc_iso(candidate)
        if parsed:
            return parsed
    return None


def parse_granule_filename(filename: str) -> GranuleName:
    """Parse canonical MODIS/VIIRS fire-product filename timestamp and collection."""
    name = Path(filename).name
    match = re.match(r"^(MOD14|MYD14|VNP14IMG|VJ114IMG|VJ214IMG)\.A(\d{4})(\d{3})\.(\d{4})\.(\d{3})\.", name)
    if not match:
        raise ValueError(f"Unsupported Level-2 active-fire granule filename: {name}")
    product, year, doy, hhmm, collection = match.groups()
    start_at = datetime.strptime(f"{year}{doy}{hhmm}", "%Y%j%H%M").replace(tzinfo=timezone.utc)
    return GranuleName(product, name, start_at, collection)


class _AnchorParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)


class LAADSClient:
    """Small LAADS API-V2 client using an Earthdata bearer token."""

    def __init__(self, token: str, *, session: requests.Session | None = None, base_url: str = LAADS_BASE_URL):
        if not token or not token.strip():
            raise ValueError("EARTHDATA_TOKEN is required for LAADS downloads")
        self.base_url = base_url.rstrip("/")
        self.api_base = self.base_url + "/api/v2"
        self.session = session or requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token.strip()}",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "SANYAM-Research/0.2",
        })

    def search_granules(
        self, *, product: str, collection: str, start_date: date, end_date: date,
        bbox: Sequence[float], archive_set: str | None = None,
    ) -> list[str]:
        product = product.upper()
        if product not in PRODUCTS:
            raise ValueError(f"Unsupported LAADS product: {product}")
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")
        west, south, east, north = [float(v) for v in bbox]
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("invalid bbox west,south,east,north")
        # `collection` in the granule filename (061 / 002) is not the same
        # thing as LAADS' archiveSet identifier. For these products LAADS uses
        # ArchiveSet 61 for MODIS C6.1 and ArchiveSet 5200 for VIIRS C2.
        # The API documents `archiveSets` as a filter on /content/archives.
        expected_collection = str(PRODUCTS[product]["collection"])
        if str(collection).lstrip("0") != expected_collection.lstrip("0"):
            raise ValueError(
                f"{product} expects filename collection {expected_collection}, got {collection}"
            )
        selected_archive_set = str(archive_set or PRODUCTS[product]["archive_set"])
        params = {
            "products": product,
            "archiveSets": selected_archive_set,
            "temporalRanges": f"{start_date.isoformat()}..{end_date.isoformat()}",
            "regions": f"[BBOX]W{west:g} N{north:g} E{east:g} S{south:g}",
        }
        url = self.api_base + "/content/archives"
        response = self.session.get(url, params=params, timeout=(20, 120))
        if response.status_code >= 400:
            raise LAADSError(
                f"LAADS search failed for {product} (ArchiveSet {selected_archive_set}) "
                f"HTTP {response.status_code}: {response.text[:500]}"
            )
        parser = _AnchorParser()
        parser.feed(response.text)
        filenames: list[str] = []
        for href in parser.hrefs:
            candidate = urljoin(self.base_url + "/", href)
            parsed = urlparse(candidate)
            if parsed.netloc and parsed.netloc != urlparse(self.base_url).netloc:
                continue
            path = parsed.path
            if path.endswith((".hdf", ".nc", ".h5", ".hdf5")):
                filenames.append(candidate)
        # Some API revisions return a JSON envelope for archive queries.
        if not filenames:
            try:
                payload = response.json()
            except ValueError:
                payload = None
            if payload is not None:
                for item in _walk_json_items(payload):
                    candidate = item.get("url") or item.get("downloadUrl") or item.get("href") or item.get("path") or item.get("fileName")
                    if candidate and str(candidate).endswith((".hdf", ".nc", ".h5", ".hdf5")):
                        filenames.append(urljoin(self.base_url + "/", str(candidate)))
        return sorted(set(filenames))

    def download(self, url_or_path: str, destination: Path) -> Path:
        url = urljoin(self.base_url + "/", url_or_path)
        parsed = urlparse(url)
        if parsed.netloc != urlparse(self.base_url).netloc:
            raise ValueError("refusing LAADS download URL outside the configured host")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_suffix(destination.suffix + ".part")
        try:
            with self.session.get(url, stream=True, timeout=(20, 300)) as response:
                if response.status_code >= 400:
                    raise LAADSError(f"LAADS download failed HTTP {response.status_code} for {destination.name}")
                with temp.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            if not temp.exists() or temp.stat().st_size == 0:
                raise LAADSError(f"LAADS returned an empty download for {destination.name}")
            temp.replace(destination)
            return destination
        except Exception:
            temp.unlink(missing_ok=True)
            raise


def _walk_json_items(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json_items(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json_items(child)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_scalar(value: Any) -> Any:
    if isinstance(value, np.ma.MaskedArray):
        if np.ma.is_masked(value):
            return None
        value = value.item() if value.ndim == 0 else value.tolist()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").rstrip("\x00")
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _finite_float(value: Any) -> float | None:
    try:
        value = float(_safe_scalar(value))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _native_attrs(attrs: Mapping[str, Any]) -> dict[str, Any]:
    return {str(k): _safe_scalar(v) for k, v in attrs.items()}


def _time_for_pixel(start_at: datetime, end_at: datetime | None, line: int | None, nlines: int) -> tuple[datetime, str]:
    if end_at is None or line is None or nlines < 2:
        return start_at, "GRANULE_START_ONLY"
    fraction = min(1.0, max(0.0, int(line) / max(1, nlines - 1)))
    value = start_at + (end_at - start_at) * fraction
    return value, "LINEAR_INTERPOLATED_FROM_GRANULE_TIME_BOUNDS"


def _lookup(arrays: Mapping[str, Any], *names: str) -> Any | None:
    normalized = {str(k).strip().lower().replace("_", " "): v for k, v in arrays.items()}
    for name in names:
        key = name.strip().lower().replace("_", " ")
        if key in normalized:
            return normalized[key]
    return None


def _pixel_footprint_wkt(arrays: Mapping[str, Any], line: int, sample: int) -> str | None:
    """Build a pixel polygon only from explicit 4-corner grids/arrays.

    A pair of center-coordinate arrays is intentionally not relabelled as true
    pixel corners. A future derived-center implementation can use a different
    geometry_source and must carry an approximation warning.
    """
    lat_corners = _lookup(arrays, "latitude_corners", "pixel_corner_latitude", "corner_latitude")
    lon_corners = _lookup(arrays, "longitude_corners", "pixel_corner_longitude", "corner_longitude")
    if lat_corners is None or lon_corners is None:
        return None
    lat = np.asarray(lat_corners, dtype=float)
    lon = np.asarray(lon_corners, dtype=float)
    try:
        if lat.ndim == 3 and lat.shape[-1] == 4 and lon.shape == lat.shape:
            coords = [(lon[line, sample, i], lat[line, sample, i]) for i in range(4)]
        elif lat.ndim == 2 and lon.shape == lat.shape and line + 1 < lat.shape[0] and sample + 1 < lat.shape[1]:
            coords = [
                (lon[line, sample], lat[line, sample]),
                (lon[line, sample + 1], lat[line, sample + 1]),
                (lon[line + 1, sample + 1], lat[line + 1, sample + 1]),
                (lon[line + 1, sample], lat[line + 1, sample]),
            ]
        else:
            return None
        if not all(np.isfinite(x) and np.isfinite(y) for x, y in coords):
            return None
        from shapely.geometry import Polygon
        polygon = Polygon(coords)
        if polygon.is_empty or not polygon.is_valid or polygon.area <= 0:
            return None
        return polygon.wkt
    except (IndexError, TypeError, ValueError):
        return None


def _normalise_fire_observation(
    *, product: str, granule: GranuleName, attrs: Mapping[str, Any], arrays: Mapping[str, Any],
    raw_asset_id: int, raw_sha256: str, storage_path: str, index: int,
    line: int | None, sample: int | None, lat: float, lon: float,
    field_values: Mapping[str, Any], fire_mask_class: int | None, algorithm_qa: int | None,
) -> dict[str, Any]:
    spec = PRODUCTS[product]
    start_at = _metadata_datetime(attrs, "start") or granule.start_at
    end_at = _metadata_datetime(attrs, "end")
    if end_at and end_at < start_at:
        end_at = None
    mask = _lookup(arrays, "fire mask", "FireMask", "Fire_Mask")
    nlines = int(np.asarray(mask).shape[0]) if mask is not None and np.asarray(mask).ndim >= 2 else 0
    acquired_at, time_precision = _time_for_pixel(start_at, end_at, line, nlines)
    frp = _finite_float(field_values.get("FP_power"))
    confidence = _safe_scalar(field_values.get("FP_confidence"))
    if spec["sensor"] == "VIIRS":
        confidence = {7: "l", 8: "n", 9: "h"}.get(fire_mask_class, confidence)
    adjacent_cloud = _finite_float(field_values.get("FP_AdjCloud"))
    t4 = _finite_float(field_values.get("FP_T4"))
    t5 = _finite_float(field_values.get("FP_T5"))
    t21 = _finite_float(field_values.get("FP_T21"))
    t31 = _finite_float(field_values.get("FP_T31"))
    mean_t4 = _finite_float(field_values.get("FP_MeanT4"))
    mean_t5 = _finite_float(field_values.get("FP_MeanT5"))
    source_key = f"{granule.granule_id}|{product}|{line}|{sample}|{index}|{lat:.7f}|{lon:.7f}"
    source_hash = hashlib.sha256(source_key.encode("utf-8")).hexdigest()
    footprint = _pixel_footprint_wkt(arrays, max(0, line or 0), max(0, sample or 0))
    attrs_payload = {key: _safe_scalar(value) for key, value in field_values.items()}
    decoder_warnings = attrs.get("_SDS_READ_WARNINGS") or []
    if decoder_warnings:
        attrs_payload["_decoder_warnings"] = decoder_warnings
    native_json = json.dumps(attrs_payload, sort_keys=True, separators=(",", ":"), default=str)
    raw_json = json.dumps({
        "source": "LAADS_LEVEL2", "granule_id": granule.granule_id, "product_id": product,
        "collection": granule.collection, "storage_path": storage_path,
        "raw_asset_id": raw_asset_id, "raw_asset_sha256": raw_sha256,
        "native_attributes": attrs_payload,
    }, sort_keys=True, separators=(",", ":"), default=str)
    daynight = None
    if _finite_float(field_values.get("FP_day")) is not None:
        daynight = "D" if int(_finite_float(field_values.get("FP_day")) or 0) == 1 else "N"
    elif algorithm_qa is not None:
        daynight = "D" if ((int(algorithm_qa) >> 4) & 1) else "N"
    return {
        "source_row_hash": source_hash,
        "raw_asset_id": raw_asset_id,
        "raw_asset_sha256": raw_sha256,
        "native_attributes_json": native_json,
        "granule_id": granule.granule_id,
        "product_id": product,
        "pixel_line": line,
        "pixel_sample": sample,
        "fire_mask_class": fire_mask_class,
        "algorithm_qa": algorithm_qa,
        "adjacent_cloud_count": int(adjacent_cloud) if adjacent_cloud is not None else None,
        "native_t21_k": t21,
        "native_t31_k": t31,
        "mean_t4_k": mean_t4,
        "mean_t5_k": mean_t5,
        "acquisition_time_precision": time_precision,
        "footprint_wkt": footprint,
        "dataset_id": f"{spec['sensor']}_LAADS_{product}_{granule.collection}",
        "processing_type": "SP",
        "sensor": spec["sensor"],
        "collection": granule.collection,
        "acquired_at": acquired_at,
        "latitude": lat,
        "longitude": lon,
        "satellite": spec["platform"],
        "instrument": spec["instrument"],
        "confidence_raw": str(confidence) if confidence is not None else None,
        "frp_mw": frp,
        "brightness_k": t21 if spec["sensor"] == "MODIS" else t4,
        "bright_ti4_k": t4,
        "bright_ti5_k": t5,
        "scan": _finite_float(field_values.get("FP_scan")),
        "track": _finite_float(field_values.get("FP_track")),
        "daynight": daynight,
        "source_version": f"LAADS_COLLECTION_{granule.collection}",
        "footprint_source": "LEVEL2_PIXEL_CORNERS" if footprint else "LEVEL2_CENTER_ONLY",
        "opportunity_state": "DETECTED",
        "raw_json": raw_json,
        "native_attributes_json": native_json,
        "first_ingested_at": datetime.now(timezone.utc),
    }


def decode_modis_mask(fire_mask: Any, algorithm_qa: Any | None = None) -> dict[str, np.ndarray]:
    """Decode MODIS C6.1 Level-2 mask classes and per-pixel QA fields.

    Collection 6+ can report cloud, unknown and fire classes over water, so
    land/water attribution must come from Algorithm QA bits 0-1, not from the
    fire-mask class alone.
    """
    mask = np.asarray(fire_mask)
    if mask.ndim != 2:
        raise ValueError("MODIS fire mask must be a 2D array")
    qa = np.asarray(algorithm_qa, dtype=np.uint32) if algorithm_qa is not None else None
    if qa is not None:
        if qa.size != mask.size:
            raise ValueError("MODIS algorithm QA dimensions must match fire mask")
        qa = qa.reshape(mask.shape)
    low = mask == 7
    nominal = mask == 8
    high = mask == 9
    if qa is not None:
        land_water = qa & 0b11
        water_by_qa = land_water == 0
        coast_by_qa = land_water == 1
        land_by_qa = land_water == 2
        missing_land_water_qa = land_water == 3
        day = ((qa >> 4) & 1) == 1
        adjacent_cloud = ((qa >> 20) & 1) == 1
        adjacent_water = ((qa >> 21) & 1) == 1
        glint_level = (qa >> 22) & 0b11
        glint = glint_level > 0
    else:
        water_by_qa = np.zeros(mask.shape, dtype=bool)
        coast_by_qa = np.zeros(mask.shape, dtype=bool)
        land_by_qa = np.zeros(mask.shape, dtype=bool)
        missing_land_water_qa = np.ones(mask.shape, dtype=bool)
        day = np.zeros(mask.shape, dtype=bool)
        adjacent_cloud = np.zeros(mask.shape, dtype=bool)
        adjacent_water = np.zeros(mask.shape, dtype=bool)
        glint_level = np.zeros(mask.shape, dtype=np.uint8)
        glint = np.zeros(mask.shape, dtype=bool)
    # MODIS mask class 5 is explicitly non-fire land. QA bits 0-1 are still
    # retained to distinguish water/coast for classes that can occur over water
    # (cloud, unknown, and fire), but a class-5 pixel remains clear land.
    clear_land = mask == 5
    return {
        "not_processed": np.isin(mask, [0, 1, 2]), "water": (mask == 3) | water_by_qa,
        "coast_by_qa": coast_by_qa, "land_by_qa": land_by_qa,
        "water_by_qa": water_by_qa, "missing_land_water_qa": missing_land_water_qa,
        "cloud": mask == 4, "clear_land": clear_land,
        "unclassified": mask == 6, "low_confidence_fire": low,
        "nominal_confidence_fire": nominal, "high_confidence_fire": high,
        "fire": low | nominal | high, "sun_glint": glint,
        "sun_glint_level": glint_level, "daytime": day,
        "adjacent_cloud": adjacent_cloud, "adjacent_water": adjacent_water,
        "invalid_quality": np.isin(mask, [0, 1, 2, 6]) | glint,
    }


def decode_viirs_mask(fire_mask: Any, algorithm_qa: Any | None = None) -> dict[str, np.ndarray]:
    """Decode VIIRS Collection 2 375 m fire-mask classes and bow-tie QA bit."""
    mask = np.asarray(fire_mask)
    if mask.ndim != 2:
        raise ValueError("VIIRS fire mask must be a 2D array")
    qa = np.asarray(algorithm_qa, dtype=np.uint32) if algorithm_qa is not None else None
    if qa is not None:
        if qa.size != mask.size:
            raise ValueError("VIIRS algorithm QA dimensions must match fire mask")
        qa = qa.reshape(mask.shape)
    bowtie_qa = ((qa >> 22) & 1) == 1 if qa is not None else np.zeros(mask.shape, dtype=bool)
    glint_qa = ((qa >> 17) & 1) == 1 if qa is not None else np.zeros(mask.shape, dtype=bool)
    low = mask == 7
    nominal = mask == 8
    high = mask == 9
    return {
        "not_processed": mask == 0, "bowtie_deletion": mask == 1,
        "sun_glint": mask == 2, "water": mask == 3, "cloud": mask == 4,
        "clear_land": mask == 5, "unclassified": mask == 6,
        "low_confidence_fire": low, "nominal_confidence_fire": nominal,
        "high_confidence_fire": high, "fire": low | nominal | high,
        "bowtie_residual_qa": bowtie_qa, "sun_glint_qa": glint_qa,
        "invalid_quality": np.isin(mask, [0, 1, 2, 6]) | bowtie_qa | glint_qa,
    }


def _hdf4_numpy_dtype(type_code: Any, sdc: Any) -> np.dtype:
    """Map common HDF4 numeric types to NumPy dtypes for empty SDS arrays."""
    candidates = (
        ("INT8", np.int8), ("UINT8", np.uint8), ("CHAR8", np.int8),
        ("UCHAR8", np.uint8), ("INT16", np.int16), ("UINT16", np.uint16),
        ("INT32", np.int32), ("UINT32", np.uint32),
        ("FLOAT32", np.float32), ("FLOAT64", np.float64),
        ("INT64", np.int64), ("UINT64", np.uint64),
    )
    for constant_name, dtype in candidates:
        constant = getattr(sdc, constant_name, None)
        if constant is not None and int(constant) == int(type_code):
            return np.dtype(dtype)
    # There are no values in a zero-length array, so float64 is a safe
    # neutral representation when the HDF4 numeric type is not recognized.
    return np.dtype(np.float64)


def _read_hdf4(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read HDF4 SDS arrays, treating declared zero-length fire vectors as empty.

    MOD14/MYD14 store the fire-pixel table as a set of 1-D SDS vectors. When a
    granule has no fire detections, those vectors can have a zero-length shape;
    pyhdf may raise SDreaddata failure when asked to read such an SDS. The SDS
    metadata (including dimensions) is authoritative in that case, so create
    a correctly shaped empty array without attempting a data read.

    Non-empty unreadable optional layers are retained as warnings. The fire
    mask is mandatory, and non-empty unreadable geolocation/index vectors must
    not be silently treated as an empty fire table.
    """
    try:
        from pyhdf.SD import SD, SDC
    except ImportError as exc:
        raise LAADSError("pyhdf is required for MOD14/MYD14 HDF4 files; install the HDF4 system library and pyhdf") from exc
    handle = SD(str(path), SDC.READ)
    try:
        arrays: dict[str, Any] = {}
        read_warnings: list[dict[str, str]] = []
        empty_sds: list[str] = []
        required_vectors = {"fp_line", "fp_sample", "fp_latitude", "fp_longitude"}
        catalog = handle.datasets()
        for name, descriptor in catalog.items():
            dataset = None
            dims: tuple[int, ...] | None = None
            type_code: Any = None
            if isinstance(descriptor, (tuple, list)) and len(descriptor) >= 3:
                try:
                    dims = tuple(int(value) for value in descriptor[1])
                    type_code = descriptor[2]
                except (TypeError, ValueError):
                    dims = None
            if dims is not None and dims and any(size == 0 for size in dims):
                arrays[name] = np.empty(dims, dtype=_hdf4_numpy_dtype(type_code, SDC))
                empty_sds.append(str(name))
                LOG.debug("HDF4 SDS %s in %s is declared zero-length; treating as empty", name, path.name)
                continue
            try:
                dataset = handle.select(name)
                arrays[name] = dataset.get()
            except Exception as exc:
                warning = {
                    "code": "HDF4_SDS_READ_FAILED",
                    "dataset": str(name),
                    "message": f"Could not read HDF4 SDS {name!r}: {type(exc).__name__}: {exc}",
                }
                read_warnings.append(warning)
                LOG.warning("Could not read HDF4 SDS %s from %s: %s", name, path.name, exc)
                normalized_name = str(name).strip().lower().replace("_", " ")
                if normalized_name in {item.replace("_", " ") for item in required_vectors}:
                    # A failed non-empty (or dimension-unknown) required vector
                    # means we cannot safely conclude the granule has no fires.
                    raise LAADSError(
                        f"Required MODIS fire-pixel SDS {name!r} is non-empty or has "
                        "unknown dimensions but could not be read; refusing to classify this granule as zero-fire"
                    ) from exc
            finally:
                if dataset is not None:
                    endaccess = getattr(dataset, "endaccess", None)
                    if callable(endaccess):
                        try:
                            endaccess()
                        except Exception:
                            LOG.debug("Could not close HDF4 SDS %s", name, exc_info=True)

        if _lookup(arrays, "fire mask") is None:
            failed_mask = any(
                str(item["dataset"]).strip().lower().replace("_", " ") == "fire mask"
                for item in read_warnings
            )
            reason = "could not be read" if failed_mask else "is absent"
            raise LAADSError(
                f"Required MODIS HDF4 'fire mask' SDS {reason}; "
                "granule is not usable for Level-2 opportunity classification"
            )

        attrs = {key: _safe_scalar(value) for key, value in _native_attrs(handle.attributes()).items()}
        if read_warnings:
            attrs["_SDS_READ_WARNINGS"] = read_warnings
        if empty_sds:
            attrs["_EMPTY_SDS_NAMES"] = empty_sds
        return arrays, attrs
    finally:
        handle.end()


def _read_hdf5(path: Path, *, sparse_ingest: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read a VIIRS NetCDF4/HDF5 granule without duplicate materialization.

    The previous implementation evaluated ``variable[...]`` twice for each
    NetCDF variable (once to inspect ``filled`` and again to obtain data). On
    large VIIRS swaths this temporarily doubled peak allocation. Read each SDS
    exactly once. For sparse-observation ingestion, load only the fire mask,
    algorithm QA, fire-pixel vectors and explicitly named corner arrays; other
    science layers remain intact in the immutable raw granule on disk and can
    be decoded later by a dedicated analysis path.
    """
    def wanted_variable(variable_name: str) -> bool:
        if not sparse_ingest:
            return True
        base_name = str(variable_name).split("/")[-1]
        normalized = base_name.strip().lower().replace("_", " ")
        compact = normalized.replace(" ", "")
        return (
            compact in {"firemask", "algorithmqa"}
            or normalized.startswith("fp ")
            or compact.startswith("fp")
            or normalized in {
                "latitude corners", "longitude corners",
                "pixel corner latitude", "pixel corner longitude",
                "corner latitude", "corner longitude",
            }
        )

    try:
        import netCDF4
        with netCDF4.Dataset(path, "r") as dataset:
            arrays: dict[str, Any] = {}
            for name, variable in dataset.variables.items():
                if not wanted_variable(name):
                    continue
                value = variable[...]
                if np.ma.isMaskedArray(value):
                    # Fill masked cells with NaN for floating data; integer
                    # categorical layers retain their native dtype and use the
                    # declared fill value only where masked.
                    if np.issubdtype(value.dtype, np.floating):
                        value = value.filled(np.nan)
                    else:
                        fill_value = getattr(variable, "_FillValue", None)
                        value = value.filled(fill_value if fill_value is not None else 0)
                arrays[name.split("/")[-1]] = np.asarray(value)
            attrs = {name: _safe_scalar(dataset.getncattr(name)) for name in dataset.ncattrs()}
            return arrays, attrs
    except ImportError:
        pass
    except OSError:
        # Try generic HDF5 access for valid HDF5 files not exposed by netCDF4.
        pass
    try:
        import h5py
    except ImportError as exc:
        raise LAADSError("netCDF4 or h5py is required for VIIRS NetCDF4/HDF5 files") from exc
    arrays: dict[str, Any] = {}
    attrs: dict[str, Any] = {}
    with h5py.File(path, "r") as handle:
        attrs = {key: _safe_scalar(value) for key, value in handle.attrs.items()}
        def visit(name: str, item: Any) -> None:
            if isinstance(item, h5py.Dataset) and wanted_variable(name):
                # Materialize each selected HDF5 dataset once. Raw bytes remain
                # preserved in the immutable granule file on disk.
                arrays[name.split("/")[-1]] = np.asarray(item[()])
                for key, value in item.attrs.items():
                    arrays.setdefault(f"{name.split('/')[-1]}__attr__{key}", _safe_scalar(value))
        handle.visititems(visit)
    return arrays, attrs


def _validate_fire_pixel_vectors(
    sensor: str, vectors: Mapping[str, Any | None], attrs: Mapping[str, Any],
) -> int:
    """Validate the location/index vectors needed to map sparse fire rows to a mask.

    If the product declares FirePix > 0, line/sample and center latitude/longitude
    are mandatory. An unreadable vector must not silently turn a real detection
    table into an empty granule or map pixels to the wrong mask cells.
    """
    latitude = vectors.get("FP_latitude")
    observed_count = int(np.asarray(latitude).reshape(-1).size) if latitude is not None else 0
    declared_value = _finite_float(attrs.get("FirePix"))
    declared_count = int(declared_value) if declared_value is not None and declared_value >= 0 else 0
    expected_count = max(observed_count, declared_count)
    if expected_count <= 0:
        return 0
    required = ("FP_line", "FP_sample", "FP_latitude", "FP_longitude")
    missing = [name for name in required if vectors.get(name) is None]
    if missing:
        raise LAADSError(
            f"{sensor} Level-2 FirePix={expected_count} but required fire-pixel "
            f"geolocation/index SDS are missing or unreadable: {', '.join(missing)}"
        )
    short = []
    for name in required:
        size = int(np.asarray(vectors[name]).reshape(-1).size)
        if size < expected_count:
            short.append(f"{name}={size}")
    if short:
        raise LAADSError(
            f"{sensor} Level-2 fire-pixel vectors contain fewer than {expected_count} "
            f"records: {', '.join(short)}"
        )
    return expected_count


def extract_modis_granule(arrays: Mapping[str, Any], attrs: Mapping[str, Any], *, product: str, granule: GranuleName, decode_full_mask: bool = True) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Pure array adapter used by pyhdf ingestion and mock-array unit tests."""
    mask = _lookup(arrays, "fire mask", "FireMask", "Fire_Mask")
    if mask is None:
        raise LAADSError("MODIS granule does not contain the required 'fire mask' SDS")
    mask = np.asarray(mask)
    if mask.ndim != 2:
        raise LAADSError("MODIS fire mask must be a 2D array")
    qa = _lookup(arrays, "algorithm QA", "AlgorithmQA", "Algorithm_QA")
    # Full boolean mask layers are needed by the swath-opportunity workflow,
    # not by sparse-detection ingestion. Avoid allocating many mask-sized
    # boolean arrays during ordinary granule ingestion.
    decoded = decode_modis_mask(mask, qa) if decode_full_mask else None
    fields = ["FP_line", "FP_sample", "FP_latitude", "FP_longitude", "FP_power", "FP_T21", "FP_T31", "FP_MeanT21", "FP_MeanT31", "FP_confidence", "FP_AdjCloud", "FP_day", "FP_SolZenAng", "FP_scan", "FP_track"]
    vectors = {field: _lookup(arrays, field) for field in fields}
    n = _validate_fire_pixel_vectors("MODIS", vectors, attrs)
    rows: list[dict[str, Any]] = []
    for i in range(n):
        item = {field: (np.asarray(value).reshape(-1)[i] if value is not None else None) for field, value in vectors.items()}
        lat, lon = _finite_float(item["FP_latitude"]), _finite_float(item["FP_longitude"])
        line, sample = _finite_float(item["FP_line"]), _finite_float(item["FP_sample"])
        if lat is None or lon is None or line is None or sample is None:
            continue
        # NASA Level-2 FP_line / FP_sample indices are zero-based for both
        # MODIS C6.1 and VIIRS C2.  Keep native coordinates unchanged.
        li, si = int(line), int(sample)
        r, c = li, si
        cls = int(mask[r, c]) if 0 <= r < mask.shape[0] and 0 <= c < mask.shape[1] else None
        qa_value = int(np.asarray(qa).reshape(-1)[r * mask.shape[1] + c]) if qa is not None and cls is not None else None
        # Keep only the fire pixels represented by the sparse table. Preserve
        # unexpected classes as rows for QC rather than silently deleting.
        rows.append({"index": i, "line": li, "sample": si, "latitude": lat, "longitude": lon,
                     "fields": item, "fire_mask_class": cls, "algorithm_qa": qa_value})
    return rows, {"fire_mask": mask, "algorithm_qa": np.asarray(qa) if qa is not None else None, "decoded_mask": decoded}


def extract_viirs_granule(arrays: Mapping[str, Any], attrs: Mapping[str, Any], *, product: str, granule: GranuleName, decode_full_mask: bool = True) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Pure array adapter for VIIRS Collection 2 VNP14IMG/VJ114IMG/VJ214IMG."""
    mask = _lookup(arrays, "fire mask", "FireMask", "Fire_Mask")
    if mask is None:
        raise LAADSError("VIIRS granule does not contain the required 'fire mask' SDS")
    mask = np.asarray(mask)
    if mask.ndim != 2:
        raise LAADSError("VIIRS fire mask must be a 2D array")
    qa = _lookup(arrays, "algorithm QA", "AlgorithmQA", "Algorithm_QA")
    # The full decoder produces several mask-sized boolean layers. During
    # sparse observation ingestion the native mask and QA are already retained
    # in the immutable raw granule; defer classification until opportunity/QC
    # requests it so a large VIIRS swath does not exhaust Codespace memory.
    decoded = decode_viirs_mask(mask, qa) if decode_full_mask else None
    fields = ["FP_line", "FP_sample", "FP_latitude", "FP_longitude", "FP_power", "FP_T4", "FP_T5", "FP_MeanT4", "FP_MeanT5", "FP_AdjCloud", "FP_confidence", "FP_day", "FP_SolZenAng"]
    vectors = {field: _lookup(arrays, field) for field in fields}
    n = _validate_fire_pixel_vectors("VIIRS", vectors, attrs)
    rows: list[dict[str, Any]] = []
    for i in range(n):
        item = {field: (np.asarray(value).reshape(-1)[i] if value is not None else None) for field, value in vectors.items()}
        lat, lon = _finite_float(item["FP_latitude"]), _finite_float(item["FP_longitude"])
        line, sample = _finite_float(item["FP_line"]), _finite_float(item["FP_sample"])
        if lat is None or lon is None or line is None or sample is None:
            continue
        # NASA Level-2 FP_line / FP_sample indices are zero-based for both
        # MODIS C6.1 and VIIRS C2.  Keep native coordinates unchanged.
        li, si = int(line), int(sample)
        r, c = li, si
        cls = int(mask[r, c]) if 0 <= r < mask.shape[0] and 0 <= c < mask.shape[1] else None
        qa_value = int(np.asarray(qa).reshape(-1)[r * mask.shape[1] + c]) if qa is not None and cls is not None else None
        rows.append({"index": i, "line": li, "sample": si, "latitude": lat, "longitude": lon,
                     "fields": item, "fire_mask_class": cls, "algorithm_qa": qa_value})
    return rows, {"fire_mask": mask, "algorithm_qa": np.asarray(qa) if qa is not None else None, "decoded_mask": decoded}


def _extract_file(
    path: Path, product: str, granule: GranuleName, *, decode_full_mask: bool = True,
    sparse_ingest: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    if product in {"MOD14", "MYD14"}:
        arrays, attrs = _read_hdf4(path)
        rows, mask_data = extract_modis_granule(
            arrays, attrs, product=product, granule=granule, decode_full_mask=decode_full_mask,
        )
    else:
        arrays, attrs = _read_hdf5(path, sparse_ingest=sparse_ingest)
        rows, mask_data = extract_viirs_granule(
            arrays, attrs, product=product, granule=granule, decode_full_mask=decode_full_mask,
        )
    return rows, mask_data, {**attrs, "_arrays": arrays}


def _platform_raw_directory(product: str, raw_root: Path) -> Path:
    spec = PRODUCTS[product]
    sensor = spec["sensor"].lower()
    platform_key = {
        "Terra": "terra", "Aqua": "aqua", "Suomi-NPP": "snpp",
        "NOAA-20": "noaa20", "NOAA-21": "noaa21",
    }[spec["platform"]]
    return raw_root / sensor / platform_key


def ingest_laads(
    repository: DatabaseRepository, *, client: LAADSClient, products: Sequence[str],
    start_date: date, end_date: date, bbox: Sequence[float], raw_root: Path = Path("data/raw"),
) -> dict[str, Any]:
    """Discover/download/parse Level-2 files and insert them into shared tables."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    output: dict[str, Any] = {"products": {}, "warnings": []}
    for product in products:
        product = product.upper()
        if product not in PRODUCTS:
            raise ValueError(f"Unsupported product: {product}")
        spec = PRODUCTS[product]
        diag = platform_availability_diagnostics(
            [spec["platform"]],
            datetime.combine(start_date, time.min, tzinfo=timezone.utc),
            datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc),
        )
        output["warnings"].extend(diag)
        # Requests entirely outside the operational interval are not sent.
        if diag and diag[0]["code"] == "PLATFORM_OUT_OF_OPERATIONAL_RANGE":
            output["products"][product] = {"status": "SKIPPED_OUT_OF_OPERATIONAL_RANGE", "warnings": diag}
            continue
        try:
            granule_urls = client.search_granules(
                product=product, collection=spec["collection"], archive_set=spec["archive_set"],
                start_date=start_date, end_date=end_date, bbox=bbox,
            )
        except Exception as exc:
            message = f"{product} discovery failed (ArchiveSet {spec['archive_set']}): {exc}"
            LOG.exception("LAADS product search failed for %s", product)
            output["products"][product] = {
                "status": "FAILED", "archive_set": spec["archive_set"],
                "filename_collection": spec["collection"], "granules_discovered": 0,
                "granules_downloaded_or_cached": 0, "observations_inserted": 0,
                "duplicates": 0, "rejected_granules": 0, "errors": [message],
            }
            output["warnings"].append({
                "code": "LAADS_PRODUCT_SEARCH_FAILED", "product": product,
                "archive_set": spec["archive_set"], "message": message,
            })
            # A failure for one product must not prevent other requested
            # platforms/products from being discovered and ingested.
            continue
        bbox_text = ",".join(str(float(v)) for v in bbox)
        ingestion_run_id = repository.create_run(
            dataset_id=f"LAADS_{product}_{spec['collection']}",
            start_date=start_date, end_date=end_date, bbox_value=bbox_text,
        )
        fetched = inserted = duplicates = rejected = 0
        errors: list[str] = []
        for url in granule_urls:
            name = Path(urlparse(url).path).name
            try:
                granule = parse_granule_filename(name)
                if granule.collection.lstrip("0") != spec["collection"].lstrip("0"):
                    raise LAADSError(f"Granule collection {granule.collection} does not match expected {spec['collection']}")
                destination = _platform_raw_directory(product, raw_root) / name
                if not destination.exists():
                    client.download(url, destination)
                fetched += 1
                checksum = sha256_file(destination)
                byte_size = destination.stat().st_size
                try:
                    observations_from_file, masks, all_info = _extract_file(
                        destination, product, granule, decode_full_mask=False,
                        sparse_ingest=True,
                    )
                    attrs = {k: v for k, v in all_info.items() if k != "_arrays"}
                    start_at = _metadata_datetime(attrs, "start") or granule.start_at
                    end_at = _metadata_datetime(attrs, "end")
                    if end_at and end_at < start_at:
                        end_at = None
                    raw_asset_id = repository.create_raw_asset({
                        "ingestion_run_id": ingestion_run_id,
                        "asset_role": "LAADS_LEVEL2_GRANULE",
                        "storage_uri": str(destination.resolve()),
                        "sha256": checksum,
                        "media_type": "application/x-hdf4" if product in {"MOD14", "MYD14"} else "application/x-netcdf4",
                        "byte_size": byte_size,
                        "file_size": byte_size,
                        "granule_id": granule.granule_id,
                        "product_id": product,
                        "collection_id": spec["collection"],
                        "platform": spec["platform"],
                        "acquisition_start": start_at,
                        "acquisition_end": end_at,
                    })
                    mask = masks["fire_mask"]
                    observation_rows = []
                    for item in observations_from_file:
                        observation_rows.append(_normalise_fire_observation(
                            product=product, granule=granule, attrs=attrs,
                            arrays=all_info["_arrays"], raw_asset_id=raw_asset_id,
                            raw_sha256=checksum, storage_path=str(destination.resolve()),
                            index=item["index"], line=item["line"], sample=item["sample"],
                            lat=item["latitude"], lon=item["longitude"],
                            field_values=item["fields"], fire_mask_class=item["fire_mask_class"],
                            algorithm_qa=item["algorithm_qa"],
                        ))
                    n_inserted, n_duplicates = repository.insert_observations(ingestion_run_id, observation_rows)
                    inserted += n_inserted
                    duplicates += n_duplicates
                except Exception:
                    rejected += 1
                    raise
            except Exception as exc:
                errors.append(f"{name}: {exc}")
                LOG.exception("Could not ingest LAADS granule %s", name)
            finally:
                # Release references to full swath arrays before opening the
                # next granule. LAADS files remain immutable on disk, so this
                # does not discard source evidence.
                observations_from_file = None
                masks = None
                all_info = None
                attrs = None
                mask = None
                observation_rows = None
                gc.collect()
                # Ask glibc to return freed NumPy buffers to the OS where
                # available; this is optional and harmless on other runtimes.
                try:
                    import ctypes
                    trim = getattr(ctypes.CDLL(None), "malloc_trim", None)
                    if trim is not None:
                        trim(0)
                except Exception:
                    pass
        status = "COMPLETED" if not errors else ("PARTIAL" if inserted or fetched else "FAILED")
        repository.finish_run(
            ingestion_run_id, status=status, fetched=fetched, inserted=inserted,
            duplicates=duplicates, rejected=rejected,
            error="; ".join(errors)[:4000] if errors else None,
        )
        output["products"][product] = {
            "ingestion_run_id": ingestion_run_id, "granules_discovered": len(granule_urls),
            "granules_downloaded_or_cached": fetched, "observations_inserted": inserted,
            "duplicates": duplicates, "rejected_granules": rejected,
            "status": status, "errors": errors,
        }
    return output



def iter_level2_swaths_for_opportunity(
    repository: DatabaseRepository,
    *,
    sensor: str,
    start_at: datetime,
    end_at: datetime,
    bbox: Sequence[float],
    platforms: Sequence[str] | None = None,
    limit: int = 500,
) -> Iterable[dict[str, Any]]:
    """Lazily decode local LAADS granules for swath-opportunity evaluation.

    Raw files are streamed one granule at a time. A swath with no explicit
    corner geolocation is still yielded for positive-detection evidence, but
    its mask cannot prove VALID_NO_DETECTION or coverage. A granule without
    both start and end times likewise cannot prove a no-detection interval.
    """
    sensor_value = sensor.upper().strip()
    if sensor_value not in {"MODIS", "VIIRS"}:
        raise ValueError("sensor must be MODIS or VIIRS")
    start_at = start_at.astimezone(timezone.utc)
    end_at = end_at.astimezone(timezone.utc)
    selected_platforms = [canonical_platform(value) for value in platforms] if platforms else None
    assets = repository.fetch_raw_assets(
        start_at=start_at, end_at=end_at, platforms=selected_platforms, limit=limit
    )
    product_by_id = {name: value for name, value in PRODUCTS.items()}
    for asset in assets:
        product = str(asset.get("product_id") or "").upper()
        spec = product_by_id.get(product)
        if not spec or spec["sensor"] != sensor_value:
            continue
        path = Path(str(asset.get("storage_uri") or ""))
        if not path.is_file():
            LOG.warning("Raw LAADS granule missing on disk: %s", path)
            continue
        try:
            granule = parse_granule_filename(path.name)
            observations, mask_data, info = _extract_file(path, product, granule)
            arrays = info["_arrays"]
            corner_latitude = _lookup(arrays, "latitude_corners", "pixel_corner_latitude", "corner_latitude")
            corner_longitude = _lookup(arrays, "longitude_corners", "pixel_corner_longitude", "corner_longitude")
            attrs = {key: value for key, value in info.items() if key != "_arrays"}
            swath_start = asset.get("acquisition_start") or _metadata_datetime(attrs, "start") or granule.start_at
            swath_end = asset.get("acquisition_end") or _metadata_datetime(attrs, "end")
            yield {
                "sensor": sensor_value,
                "platform": asset.get("platform") or spec["platform"],
                "granule_id": asset.get("granule_id") or granule.granule_id,
                "product_id": product,
                "collection": asset.get("collection_id") or spec["collection"],
                "raw_asset_id": asset.get("raw_asset_id"),
                "sha256": asset.get("sha256"),
                "storage_uri": str(path.resolve()),
                "fire_mask": mask_data["fire_mask"],
                "algorithm_qa": mask_data.get("algorithm_qa"),
                "corner_latitude": corner_latitude,
                "corner_longitude": corner_longitude,
                "start_at": swath_start,
                "end_at": swath_end,
                "bbox": bbox,
                "observations_in_granule": observations,
            }
        except Exception as exc:
            LOG.exception("Unable to decode Level-2 swath %s", path.name)
            yield {
                "sensor": sensor_value, "platform": asset.get("platform") or spec["platform"],
                "granule_id": asset.get("granule_id") or path.name,
                "product_id": product, "collection": asset.get("collection_id") or spec["collection"],
                "raw_asset_id": asset.get("raw_asset_id"), "sha256": asset.get("sha256"),
                "storage_uri": str(path.resolve()), "decode_error": str(exc),
                "start_at": asset.get("acquisition_start"), "end_at": asset.get("acquisition_end"),
                "bbox": bbox,
            }


def _parse_products(value: str) -> list[str]:
    products = [part.strip().upper() for part in value.split(",") if part.strip()]
    if not products or any(product not in PRODUCTS for product in products):
        raise argparse.ArgumentTypeError(f"products must be a comma-separated subset of {','.join(PRODUCTS)}")
    return products


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SANYAM NASA LAADS DAAC Level-2 active-fire ingestion")
    sub = parser.add_subparsers(dest="command", required=True)
    ingest = sub.add_parser("ingest", help="Search, download and ingest LAADS Level-2 granules")
    ingest.add_argument("--products", type=_parse_products, default=list(PRODUCTS), help="comma-separated products")
    ingest.add_argument("--start-date", required=True)
    ingest.add_argument("--end-date", required=True)
    ingest.add_argument("--bbox", required=True, help="west,south,east,north")
    ingest.add_argument("--raw-root", default="data/raw")
    ingest.add_argument("--database-url", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s | %(levelname)s | %(message)s")
    args = build_parser().parse_args(argv)
    token = os.getenv("EARTHDATA_TOKEN")
    if not token:
        print("ERROR: EARTHDATA_TOKEN is not set in .env or the environment", file=sys.stderr)
        return 2
    start_date, end_date = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    bbox = [float(v) for v in args.bbox.split(",")]
    repo = connect_db(args.database_url)
    try:
        result = ingest_laads(
            repo, client=LAADSClient(token), products=args.products,
            start_date=start_date, end_date=end_date, bbox=bbox, raw_root=Path(args.raw_root),
        )
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0 if all(v.get("status") in {"COMPLETED", "SKIPPED_OUT_OF_OPERATIONAL_RANGE"} for v in result["products"].values()) else 1
    except Exception as exc:
        LOG.exception("LAADS ingestion failed")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
