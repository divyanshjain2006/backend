#!/usr/bin/env python3
"""SANYAM Module 4 (Quality Control) and Module 5 (Observation Opportunity).

Implements the FIRMS-CSV milestone described in SKH Master Documentation
§§6.6, 6.7 and 6.10. All database operations go through db.DatabaseRepository,
which supports both SQLite and PostgreSQL.

Scientific boundary: FIRMS active-fire CSVs contain positive hotspot records,
not full swath masks. Therefore this provider never infers VALID_NO_DETECTION,
CLOUD_OBSCURED, or NOT_COVERED from an empty FIRMS query. Such states are
reserved for a future Level-2 swath/quality provider.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

import numpy as np

from sanyam_firms_portable.db import DatabaseRepository, connect_db

try:
    from dotenv import load_dotenv
except ImportError:  # .env is optional; environment variables also work.
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

LOG = logging.getLogger("sanyam.qc_opportunity")
DEFAULT_QC_VERSION = "1.0.0"
DEFAULT_OPPORTUNITY_VERSION = "1.0.0"
DEFAULT_BATCH_SIZE = 1000
FIRMS_UNKNOWN_NOTE = (
    "FIRMS point-only CSV cannot distinguish valid non-detection from cloud cover or orbital gap"
)

QUALITY_STATUSES = {
    "VALID", "VALID_WITH_WARNINGS", "INVALID", "INCOMPLETE", "QUARANTINED"
}
OPPORTUNITY_STATES = {
    "DETECTED", "VALID_NO_DETECTION", "CLOUD_OBSCURED",
    "INVALID_QUALITY", "NOT_COVERED", "UNKNOWN",
}


@dataclass(frozen=True)
class BoundingBox:
    west: float
    south: float
    east: float
    north: float

    @classmethod
    def parse(cls, value: str | Sequence[float] | "BoundingBox") -> "BoundingBox":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            parts = value.split(",")
            if len(parts) != 4:
                raise ValueError("bbox must be west,south,east,north")
            values = [float(part.strip()) for part in parts]
        else:
            values = [float(part) for part in value]
        if len(values) != 4:
            raise ValueError("bbox must contain west,south,east,north")
        west, south, east, north = values
        if not all(math.isfinite(v) for v in values):
            raise ValueError("bbox coordinates must be finite")
        if not (-180 <= west < east <= 180):
            raise ValueError("bbox longitude must satisfy -180 <= west < east <= 180")
        if not (-90 <= south < north <= 90):
            raise ValueError("bbox latitude must satisfy -90 <= south < north <= 90")
        return cls(west, south, east, north)

    @property
    def spatial_unit_id(self) -> str:
        return "BBOX:" + ",".join(f"{v:.6f}".rstrip("0").rstrip(".") for v in (
            self.west, self.south, self.east, self.north
        ))


def _as_utc_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        # Database UTCDateTime should return aware timestamps. For legacy source
        # rows only, interpret a naive stored value as UTC, never local time.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _confidence_category(sensor: str, confidence_raw: Any) -> tuple[str, list[dict[str, str]], bool]:
    """Return category, warnings, and whether confidence metadata is complete."""
    raw = "" if confidence_raw is None else str(confidence_raw).strip()
    normalized_sensor = str(sensor or "").upper()
    if not raw:
        return "UNKNOWN", [_warning("MISSING_CONFIDENCE", "Source confidence is unavailable")], False

    if normalized_sensor == "MODIS":
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return "UNKNOWN", [_warning(
                "INVALID_MODIS_CONFIDENCE", f"MODIS confidence is not numeric: {raw!r}"
            )], False
        if not math.isfinite(value) or not 0 <= value <= 100:
            return "UNKNOWN", [_warning(
                "INVALID_MODIS_CONFIDENCE", f"MODIS confidence must be in 0..100: {raw!r}"
            )], False
        if value < 30:
            return "LOW", [_warning(
                "LOW_CONFIDENCE_MODIS", f"MODIS confidence {value:g} is below 30"
            )], True
        if value < 80:
            return "NOMINAL", [], True
        return "HIGH", [], True

    if normalized_sensor == "VIIRS":
        mapping = {
            "l": "LOW", "low": "LOW",
            "n": "NOMINAL", "nominal": "NOMINAL",
            "h": "HIGH", "high": "HIGH",
        }
        category = mapping.get(raw.casefold())
        if category is None:
            return "UNKNOWN", [_warning(
                "UNRECOGNIZED_VIIRS_CONFIDENCE",
                f"VIIRS confidence label is not recognized: {raw!r}",
            )], False
        if category == "LOW":
            return category, [_warning(
                "LOW_CONFIDENCE_VIIRS", f"VIIRS confidence label {raw!r} is low"
            )], True
        return category, [], True

    return "UNKNOWN", [_warning(
        "UNKNOWN_SENSOR", f"Cannot interpret confidence for sensor {sensor!r}"
    )], False


def evaluate_observation_qc(observation: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate one observation without changing or deleting the source row.

    Coordinates and acquisition time determine point/time matching suitability.
    FRP suitability is independent and additionally requires positive FRP.
    Nonpositive scan/track values are warnings: they prevent trusting a
    footprint derived from those fields but do not invalidate point matching.
    """
    warnings: list[dict[str, str]] = []
    invalid_codes: list[str] = []
    incomplete_codes: list[str] = []
    quarantined = False
    decoded_raw: dict[str, Any] = {}

    observation_id = observation.get("observation_id")
    sensor = str(observation.get("sensor") or "").upper()

    # Stage 1: raw evidence integrity and required structure.
    raw_json = observation.get("raw_json")
    if not _is_present(raw_json):
        quarantined = True
        warnings.append(_warning("MISSING_RAW_SOURCE_RECORD", "raw_json is missing; source evidence requires inspection"))
    else:
        try:
            decoded = json.loads(str(raw_json))
            if not isinstance(decoded, dict):
                raise ValueError("raw_json must contain a JSON object")
            decoded_raw = decoded
        except (json.JSONDecodeError, TypeError, ValueError):
            quarantined = True
            warnings.append(_warning("MALFORMED_RAW_JSON", "raw_json is not a valid JSON object"))

    if not observation_id:
        incomplete_codes.append("MISSING_OBSERVATION_ID")
        warnings.append(_warning("MISSING_OBSERVATION_ID", "Observation identifier is unavailable"))

    if sensor not in {"MODIS", "VIIRS"}:
        incomplete_codes.append("UNKNOWN_SENSOR")
        warnings.append(_warning("UNKNOWN_SENSOR", f"Sensor is missing or unsupported: {sensor!r}"))

    dataset_id = str(observation.get("dataset_id") or "").upper()
    if not dataset_id:
        incomplete_codes.append("MISSING_DATASET_ID")
        warnings.append(_warning("MISSING_DATASET_ID", "Product dataset identifier is unavailable"))
    elif sensor and not dataset_id.startswith(sensor):
        incomplete_codes.append("SENSOR_DATASET_MISMATCH")
        warnings.append(_warning(
            "SENSOR_DATASET_MISMATCH",
            f"Sensor {sensor} does not agree with dataset_id {dataset_id}",
        ))

    # Level-2 native mask/QA diagnostics. Raw classes and QA bits are preserved;
    # these warnings never delete or rewrite a source observation.
    is_level2 = decoded_raw.get("source") == "LAADS_LEVEL2" or observation.get("raw_asset_id") is not None
    if is_level2:
        mask_class = observation.get("fire_mask_class")
        qa_value = observation.get("algorithm_qa")
        try:
            mask_class = int(mask_class) if mask_class is not None else None
        except (TypeError, ValueError):
            mask_class = None
        try:
            qa_value = int(qa_value) if qa_value is not None else None
        except (TypeError, ValueError):
            qa_value = None
        adj_cloud = observation.get("adjacent_cloud_count")
        try:
            if adj_cloud is not None and float(adj_cloud) > 0:
                warnings.append(_warning(
                    "ADJACENT_CLOUD_CONTAMINATION",
                    f"Fire pixel has {int(float(adj_cloud))} adjacent cloud pixel(s)",
                ))
        except (TypeError, ValueError):
            warnings.append(_warning("INVALID_ADJACENT_CLOUD_COUNT", "FP_AdjCloud is not numeric"))
        if mask_class == 7:
            warnings.append(_warning(
                "LOW_CONFIDENCE_FIRE_MASK",
                "Native fire mask identifies a low-confidence fire pixel; retain but review separately",
            ))
        if sensor == "VIIRS" and mask_class == 1:
            warnings.append(_warning(
                "BOWTIE_EDGE_PIXEL",
                "VIIRS fire-mask class 1 identifies a bow-tie deletion pixel; do not interpret it as observed clear land",
            ))
        if sensor == "VIIRS" and mask_class == 2:
            warnings.append(_warning(
                "SUN_GLINT_QA_FLAG",
                "VIIRS fire-mask class 2 identifies a sun-glint pixel; exclude it from valid clear-land opportunity",
            ))
        if sensor == "VIIRS" and qa_value is not None and ((qa_value >> 22) & 1):
            warnings.append(_warning(
                "BOWTIE_EDGE_PIXEL",
                "VIIRS algorithm QA bit 22 marks residual bow-tie data; avoid double-counting FRP",
            ))
        if sensor == "MODIS" and qa_value is not None:
            if (qa_value >> 20) & 1:
                warnings.append(_warning(
                    "ADJACENT_CLOUD_CONTAMINATION",
                    "MODIS algorithm QA bit 20 indicates an adjacent cloud pixel",
                ))
            if ((qa_value >> 22) & 0b11) > 0:
                warnings.append(_warning(
                    "SUN_GLINT_QA_FLAG",
                    "MODIS algorithm QA reports a non-zero sun-glint level",
                ))
        if str(observation.get("acquisition_time_precision") or "").startswith("LINEAR_INTERPOLATED"):
            warnings.append(_warning(
                "GRANULE_INTERPOLATED_PIXEL_TIME",
                "Pixel time is interpolated from granule time bounds and scan line, not a native per-pixel UTC field",
            ))
        elif observation.get("acquisition_time_precision") == "GRANULE_START_ONLY":
            warnings.append(_warning(
                "GRANULE_START_TIME_ONLY",
                "No per-pixel time bounds were available; timestamp represents granule start",
            ))
        if not observation.get("footprint_wkt"):
            warnings.append(_warning(
                "PIXEL_CORNER_GEOMETRY_UNAVAILABLE",
                "No explicit 4-corner geolocation was available; centroid-only geometry must not be presented as a true footprint",
            ))

    # Stage 2: geographical validation.
    coordinate_valid = True
    for field, low, high in (("latitude", -90.0, 90.0), ("longitude", -180.0, 180.0)):
        value = observation.get(field)
        if value is None:
            coordinate_valid = False
            incomplete_codes.append(f"MISSING_{field.upper()}")
            warnings.append(_warning(f"MISSING_{field.upper()}", f"{field} is unavailable"))
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            coordinate_valid = False
            invalid_codes.append(f"INVALID_{field.upper()}")
            warnings.append(_warning(f"INVALID_{field.upper()}", f"{field} is not numeric"))
            continue
        if not math.isfinite(numeric) or not low <= numeric <= high:
            coordinate_valid = False
            invalid_codes.append(f"OUT_OF_RANGE_{field.upper()}")
            warnings.append(_warning(
                f"OUT_OF_RANGE_{field.upper()}", f"{field} must be in {low:g}..{high:g}"
            ))

    # Scan/track values are optional, but if supplied must be positive.
    for field in ("scan", "track"):
        value = observation.get(field)
        if value is None or value == "":
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            warnings.append(_warning(f"INVALID_{field.upper()}", f"{field} is not numeric; footprint use is limited"))
            continue
        if not math.isfinite(numeric) or numeric <= 0:
            warnings.append(_warning(
                f"NONPOSITIVE_{field.upper()}",
                f"{field} must be positive when present; footprint use is limited",
            ))

    # Stage 3: temporal validation. A timestamp after first_ingested_at is
    # inconsistent with the ingestion provenance and cannot support matching.
    acquired_at = _as_utc_datetime(observation.get("acquired_at"))
    first_ingested_at = _as_utc_datetime(observation.get("first_ingested_at"))
    temporal_valid = True
    if acquired_at is None:
        temporal_valid = False
        incomplete_codes.append("MISSING_OR_INVALID_ACQUIRED_AT")
        warnings.append(_warning("MISSING_OR_INVALID_ACQUIRED_AT", "acquired_at is missing or cannot be parsed"))
    if first_ingested_at is None:
        temporal_valid = False
        incomplete_codes.append("MISSING_OR_INVALID_FIRST_INGESTED_AT")
        warnings.append(_warning(
            "MISSING_OR_INVALID_FIRST_INGESTED_AT", "first_ingested_at is missing or cannot be parsed"
        ))
    if acquired_at is not None and first_ingested_at is not None and acquired_at > first_ingested_at:
        temporal_valid = False
        invalid_codes.append("ACQUISITION_AFTER_INGESTION")
        warnings.append(_warning(
            "ACQUISITION_AFTER_INGESTION",
            "acquired_at is later than first_ingested_at; check timestamp provenance",
        ))

    # Product-specific confidence interpretation. Never mutate confidence_raw.
    category, confidence_warnings, confidence_complete = _confidence_category(
        sensor, observation.get("confidence_raw")
    )
    warnings.extend(confidence_warnings)
    if not confidence_complete and sensor in {"MODIS", "VIIRS"}:
        incomplete_codes.append("INCOMPLETE_CONFIDENCE")

    # Measurement suitability is task-specific; a detection can still be
    # suitable for matching when its FRP is absent or unusable.
    frp = observation.get("frp_mw")
    frp_valid = False
    try:
        frp_value = float(frp) if frp is not None else math.nan
        frp_valid = math.isfinite(frp_value) and frp_value > 0
    except (TypeError, ValueError):
        frp_valid = False
    if not frp_valid:
        warnings.append(_warning(
            "MISSING_OR_NONPOSITIVE_FRP", "FRP is missing, non-numeric, non-finite, or <= 0 MW"
        ))

    if str(observation.get("processing_type") or "").upper() == "NRT":
        warnings.append(_warning(
            "PROVISIONAL_NRT_RECORD", "Observation uses Near Real-Time processing and is provisional"
        ))

    if quarantined:
        quality_status = "QUARANTINED"
    elif invalid_codes:
        quality_status = "INVALID"
    elif incomplete_codes:
        quality_status = "INCOMPLETE"
    elif warnings:
        quality_status = "VALID_WITH_WARNINGS"
    else:
        quality_status = "VALID"

    core_valid = coordinate_valid and temporal_valid
    suitability_for_matching = bool(core_valid and not quarantined)
    suitability_for_frp = bool(suitability_for_matching and frp_valid)

    return {
        "observation_id": observation_id,
        "quality_status": quality_status,
        "suitability_for_matching": suitability_for_matching,
        "suitability_for_frp": suitability_for_frp,
        "warnings_json": json.dumps(warnings, sort_keys=True, separators=(",", ":")),
        "source_confidence_category": category,
    }


def run_quality_control(
    repository: DatabaseRepository,
    *,
    method_version: str = DEFAULT_QC_VERSION,
    reprocess: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """QC every observation not already processed by this method version.

    Reprocessing creates a new immutable processing run and new qc_results; it
    never overwrites or deletes previous results or source observations.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    configuration = {
        "method": "SANYAM_FIRMS_QC",
        "method_version": method_version,
        "reprocess": reprocess,
        "batch_size": batch_size,
        "rule_profile": "SKH_6.6_FIRMS_CSV",
    }
    run_id = repository.create_processing_run(
        "QC", method_version, json.dumps(configuration, sort_keys=True)
    )
    input_count = 0
    output_count = 0
    status_counts: dict[str, int] = {status: 0 for status in sorted(QUALITY_STATUSES)}
    matching_count = 0
    frp_count = 0
    after_id = 0
    try:
        while True:
            batch = repository.fetch_unprocessed_observations(
                method_version=method_version,
                after_observation_id=after_id,
                limit=batch_size,
                reprocess=reprocess,
            )
            if not batch:
                break
            after_id = max(int(row["observation_id"]) for row in batch)
            results = []
            for observation in batch:
                result = evaluate_observation_qc(observation)
                result["processing_run_id"] = run_id
                results.append(result)
                status_counts[result["quality_status"]] += 1
                matching_count += int(result["suitability_for_matching"])
                frp_count += int(result["suitability_for_frp"])
            inserted = repository.insert_qc_results(results)
            input_count += len(batch)
            output_count += inserted
        repository.finish_processing_run(
            run_id,
            status="COMPLETED",
            input_count=input_count,
            output_count=output_count,
        )
    except Exception as exc:
        repository.finish_processing_run(
            run_id,
            status="FAILED",
            input_count=input_count,
            output_count=output_count,
            error_message=str(exc),
        )
        raise

    return {
        "processing_run_id": run_id,
        "module_name": "QC",
        "method_version": method_version,
        "input_count": input_count,
        "output_count": output_count,
        "status_counts": {key: value for key, value in status_counts.items() if value},
        "suitable_for_matching": matching_count,
        "suitable_for_frp": frp_count,
        "status": "COMPLETED",
    }


def _platform_name(sensor: str, raw_platform: Any) -> str:
    raw = str(raw_platform or "").strip().upper().replace("_", "-")
    mapping = {
        "T": "Terra", "TERRA": "Terra",
        "A": "Aqua", "AQUA": "Aqua",
        "N": "Suomi-NPP", "NPP": "Suomi-NPP", "SNPP": "Suomi-NPP", "SUOMI-NPP": "Suomi-NPP",
        "N20": "NOAA-20", "NOAA20": "NOAA-20", "NOAA-20": "NOAA-20",
        "N21": "NOAA-21", "NOAA21": "NOAA-21", "NOAA-21": "NOAA-21",
    }
    return mapping.get(raw, str(raw_platform).strip() if _is_present(raw_platform) else "UNKNOWN")


def _normalize_platform_filter(value: str | None) -> str | None:
    if not value:
        return None
    return _platform_name("", value)


class OpportunityProvider(Protocol):
    """Provider contract for evidence-backed opportunity classification.

    Future LAADS implementations can inspect full swath masks, geolocation,
    cloud and quality arrays and return the same record contract with states
    VALID_NO_DETECTION, CLOUD_OBSCURED, INVALID_QUALITY or NOT_COVERED.
    """

    evidence_source: str

    def build_records(
        self,
        observations: Sequence[Mapping[str, Any]],
        *,
        processing_run_id: int,
        sensor: str,
        platform_filter: str | Sequence[str] | None,
        spatial_unit_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]: ...


class FIRMSDetectionOnlyProvider:
    """Opportunity provider restricted to positive FIRMS point detections."""

    evidence_source = "FIRMS_DETECTION_ONLY"

    def build_records(
        self,
        observations: Sequence[Mapping[str, Any]],
        *,
        processing_run_id: int,
        sensor: str,
        platform_filter: str | Sequence[str] | None,
        spatial_unit_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        base = {
            "processing_run_id": processing_run_id,
            "sensor": sensor,
            "spatial_unit_id": spatial_unit_id,
            "time_window_start": start_at,
            "time_window_end": end_at,
            "evidence_source": self.evidence_source,
        }
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in observations:
            platform = _platform_name(sensor, row.get("satellite"))
            if platform_filter:
                filters = platform_filter if isinstance(platform_filter, (list, tuple, set)) else [platform_filter]
                allowed = {_normalize_platform_filter(value) for value in filters}
                if platform not in allowed:
                    continue
            grouped.setdefault(platform, []).append(row)

        if not grouped:
            filters = platform_filter if isinstance(platform_filter, (list, tuple, set)) else ([platform_filter] if platform_filter else ["UNKNOWN"])
            return [{
                **base,
                "observation_id": None,
                "platform": _normalize_platform_filter(value) if value != "UNKNOWN" else "UNKNOWN",
                "opportunity_state": "UNKNOWN",
                "notes": FIRMS_UNKNOWN_NOTE,
            } for value in filters]

        records: list[dict[str, Any]] = []
        for platform, rows in sorted(grouped.items()):
            valid_detections = 0
            invalid_quality = 0
            unknown_qc = 0
            for row in rows:
                qc_status = row.get("qc_quality_status")
                suitability = row.get("suitability_for_matching")
                if qc_status is None:
                    unknown_qc += 1
                    records.append({
                        **base,
                        "observation_id": row.get("observation_id"),
                        "platform": platform,
                        "opportunity_state": "UNKNOWN",
                        "notes": "FIRMS detection exists but no completed QC result is available; " + FIRMS_UNKNOWN_NOTE,
                    })
                elif qc_status not in {"INVALID", "QUARANTINED"} and bool(suitability):
                    valid_detections += 1
                    records.append({
                        **base,
                        "observation_id": row.get("observation_id"),
                        "platform": platform,
                        "opportunity_state": "DETECTED",
                        "notes": f"FIRMS reported a positive hotspot record; QC status={qc_status}; source confidence category={row.get('source_confidence_category') or 'UNKNOWN'}.",
                    })
                else:
                    invalid_quality += 1
                    records.append({
                        **base,
                        "observation_id": row.get("observation_id"),
                        "platform": platform,
                        "opportunity_state": "INVALID_QUALITY",
                        "notes": f"FIRMS reported a hotspot record, but its latest QC result is {qc_status}; not suitable for matching.",
                    })

            # In a group with only invalid observations, keep INVALID_QUALITY;
            # do not fabricate a valid non-detection. A true zero-row query is
            # represented by UNKNOWN above.
            LOG.debug(
                "FIRMS opportunity group %s: detected=%s invalid=%s unknown_qc=%s",
                platform, valid_detections, invalid_quality, unknown_qc,
            )
        return records


def classify_level2_swath_opportunity(
    *,
    sensor: str,
    fire_mask: Any,
    corner_latitude: Any,
    corner_longitude: Any,
    bbox: str | Sequence[float] | BoundingBox,
    algorithm_qa: Any | None = None,
    minimum_valid_area_fraction: float = 0.80,
    granule_id: str | None = None,
) -> dict[str, Any]:
    """Classify a Level-2 swath over a query bbox using pixel corner geometry.

    ``corner_latitude``/``corner_longitude`` must be explicit pixel-corner
    arrays of shape (rows+1, cols+1) or per-pixel four-corner arrays of shape
    (rows, cols, 4). Center-only geolocation is deliberately rejected here:
    it cannot prove area coverage without a documented corner construction.
    Area fractions are computed in a local Lambert Azimuthal Equal Area CRS.
    """
    from pyproj import CRS, Transformer
    from shapely.geometry import Polygon, box as shapely_box
    from shapely.ops import transform as transform_geometry

    box_value = BoundingBox.parse(bbox)
    sensor_value = str(sensor).upper().strip()
    if sensor_value not in {"MODIS", "VIIRS"}:
        raise ValueError("sensor must be MODIS or VIIRS")
    if not 0 < minimum_valid_area_fraction <= 1:
        raise ValueError("minimum_valid_area_fraction must be in (0,1]")
    mask = np.asarray(fire_mask)
    if mask.ndim != 2:
        raise ValueError("fire_mask must be 2D")
    lat = np.asarray(corner_latitude, dtype=float)
    lon = np.asarray(corner_longitude, dtype=float)
    if lat.shape != lon.shape:
        raise ValueError("latitude and longitude corner arrays must have identical shapes")
    per_pixel = lat.ndim == 3 and lat.shape == (*mask.shape, 4)
    shared_grid = lat.ndim == 2 and lat.shape == (mask.shape[0] + 1, mask.shape[1] + 1)
    if not (per_pixel or shared_grid):
        raise ValueError(
            "explicit pixel corners required: expected (rows+1,cols+1) or (rows,cols,4); "
            "center arrays are not accepted as true corners"
        )

    if sensor_value == "VIIRS":
        from sanyam_firms_portable.laads_ingest import decode_viirs_mask
        decoded = decode_viirs_mask(mask, algorithm_qa)
    else:
        from sanyam_firms_portable.laads_ingest import decode_modis_mask
        decoded = decode_modis_mask(mask, algorithm_qa)

    # Construct query polygon and pixel areas in a local equal-area CRS.
    centre_lon = (box_value.west + box_value.east) / 2.0
    centre_lat = (box_value.south + box_value.north) / 2.0
    crs = CRS.from_proj4(
        f"+proj=laea +lat_0={centre_lat} +lon_0={centre_lon} +datum=WGS84 +units=m +no_defs"
    )
    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    query_polygon = transform_geometry(
        transformer.transform,
        shapely_box(box_value.west, box_value.south, box_value.east, box_value.north),
    )
    query_area = query_polygon.area
    if query_area <= 0:
        raise ValueError("query bbox has zero projected area")

    total_covered = valid_area = cloud_area = invalid_area = fire_area = 0.0
    beta_ret = cloud_fraction = invalid_fraction = 0.0
    overlap_pixels = 0
    for row in range(mask.shape[0]):
        for col in range(mask.shape[1]):
            if per_pixel:
                coords = [(float(lon[row, col, k]), float(lat[row, col, k])) for k in range(4)]
            else:
                coords = [
                    (float(lon[row, col]), float(lat[row, col])),
                    (float(lon[row, col + 1]), float(lat[row, col + 1])),
                    (float(lon[row + 1, col + 1]), float(lat[row + 1, col + 1])),
                    (float(lon[row + 1, col]), float(lat[row + 1, col])),
                ]
            if not all(math.isfinite(x) and math.isfinite(y) for x, y in coords):
                continue
            if any(x < -180 or x > 180 or y < -90 or y > 90 for x, y in coords):
                continue
            polygon = Polygon(coords)
            if not polygon.is_valid or polygon.is_empty:
                polygon = polygon.buffer(0)
            if polygon.is_empty:
                continue
            projected = transform_geometry(transformer.transform, polygon)
            clipped_area = projected.intersection(query_polygon).area
            if clipped_area <= 0:
                continue
            overlap_pixels += 1
            total_covered += clipped_area
            cls = int(mask[row, col])
            if bool(decoded["cloud"][row, col]):
                cloud_area += clipped_area
            if bool(decoded["invalid_quality"][row, col]):
                invalid_area += clipped_area
            if bool(decoded["fire"][row, col]):
                fire_area += clipped_area
            # Valid observed area is clear land plus accepted fire-mask pixels;
            # clouds, unprocessed, bow-tie, glint and unclassified are excluded.
            if bool(decoded["clear_land"][row, col]) or bool(decoded["fire"][row, col]):
                valid_area += clipped_area

    if total_covered <= 0:
        state = "NOT_COVERED"
        reason = "No geolocated Level-2 pixel footprint intersects the requested area"
    else:
        beta_ret = min(1.0, valid_area / query_area)
        cloud_fraction = min(1.0, cloud_area / query_area)
        invalid_fraction = min(1.0, invalid_area / query_area)
        if fire_area > 0:
            state = "DETECTED"
            reason = "At least one low/nominal/high-confidence fire-mask pixel intersects the request"
        elif beta_ret >= minimum_valid_area_fraction:
            state = "VALID_NO_DETECTION"
            reason = f"Valid clear-land observation covers {beta_ret:.1%} of the requested area and no fire pixel is present"
        elif cloud_fraction > 0 and cloud_area >= max(valid_area, invalid_area):
            state = "CLOUD_OBSCURED"
            reason = f"Cloud pixels obscure {cloud_fraction:.1%} of the requested area; valid observed fraction is {beta_ret:.1%}"
        elif total_covered / query_area < minimum_valid_area_fraction:
            state = "NOT_COVERED"
            reason = f"Only {min(1.0, total_covered/query_area):.1%} of the requested area is covered by the granule"
        else:
            state = "INVALID_QUALITY"
            reason = f"Only {beta_ret:.1%} is validly observed; invalid-quality fraction is {invalid_fraction:.1%}"
    return {
        "opportunity_state": state,
        "evidence_source": "LEVEL2_SWATH_MASK",
        "beta_ret": min(1.0, valid_area / query_area),
        "valid_observed_area_km2": valid_area / 1_000_000.0,
        "covered_area_km2": total_covered / 1_000_000.0,
        "cloud_area_fraction": min(1.0, cloud_area / query_area),
        "invalid_quality_fraction": min(1.0, invalid_area / query_area),
        "fire_area_km2": fire_area / 1_000_000.0,
        "overlap_pixel_count": overlap_pixels,
        "granule_id": granule_id,
        "reason": reason,
        "geometry_source": "LEVEL2_PIXEL_CORNERS",
    }


class Level2SwathOpportunityProvider:
    """Provider for local Level-2 masks plus explicit pixel-corner geolocation.

    Each swath dictionary includes sensor, platform, granule_id, fire_mask,
    optional corner arrays, acquisition start/end, and raw-asset provenance.
    Without explicit corners or a complete time interval, no valid
    non-detection/coverage claim is made. Positive detections remain evidence
    of a detection even when the corner grid is unavailable.
    """

    evidence_source = "LEVEL2_SWATH_MASK"

    def __init__(self, swaths: Iterable[Mapping[str, Any]], *, minimum_valid_area_fraction: float = 0.80):
        self.swaths = swaths
        self.minimum_valid_area_fraction = minimum_valid_area_fraction

    def build_records(
        self, observations: Sequence[Mapping[str, Any]], *, processing_run_id: int,
        sensor: str, platform_filter: str | Sequence[str] | None,
        spatial_unit_id: str, start_at: datetime, end_at: datetime,
    ) -> list[dict[str, Any]]:
        from sanyam_firms_portable.laads_ingest import canonical_platform
        filters = platform_filter if isinstance(platform_filter, (list, tuple, set)) else ([platform_filter] if platform_filter else None)
        allowed = {canonical_platform(item) for item in filters} if filters else None
        base = {
            "processing_run_id": processing_run_id, "sensor": sensor,
            "spatial_unit_id": spatial_unit_id, "time_window_start": start_at,
            "time_window_end": end_at, "evidence_source": self.evidence_source,
        }
        records: list[dict[str, Any]] = []
        seen_platforms: set[str] = set()
        for swath in self.swaths:
            if str(swath.get("sensor", "")).upper() != sensor.upper():
                continue
            platform = canonical_platform(str(swath.get("platform")))
            if allowed and platform not in allowed:
                continue
            seen_platforms.add(platform)
            swath_start = _as_utc_datetime(swath.get("start_at"))
            swath_end = _as_utc_datetime(swath.get("end_at"))
            if swath_start and swath_end and (swath_end <= start_at or swath_start >= end_at):
                continue
            matched_rows = [row for row in observations if row.get("granule_id") == swath.get("granule_id")]
            matched_observation = next((row for row in matched_rows if row.get("sensor") == sensor), None)
            has_corners = swath.get("corner_latitude") is not None and swath.get("corner_longitude") is not None
            complete_time = swath_start is not None and swath_end is not None and swath_end > swath_start
            result: dict[str, Any] | None = None
            if has_corners and complete_time and not swath.get("decode_error"):
                result = classify_level2_swath_opportunity(
                    sensor=sensor, fire_mask=swath["fire_mask"],
                    corner_latitude=swath["corner_latitude"], corner_longitude=swath["corner_longitude"],
                    bbox=swath.get("bbox") or spatial_unit_id.removeprefix("BBOX:"),
                    algorithm_qa=swath.get("algorithm_qa"),
                    minimum_valid_area_fraction=self.minimum_valid_area_fraction,
                    granule_id=str(swath.get("granule_id") or ""),
                )
                record_start, record_end = max(start_at, swath_start), min(end_at, swath_end)
            elif matched_observation is not None:
                result = {
                    "opportunity_state": "DETECTED", "evidence_source": self.evidence_source,
                    "beta_ret": None, "valid_observed_area_km2": None,
                    "covered_area_km2": None, "cloud_area_fraction": None,
                    "invalid_quality_fraction": None,
                    "reason": "A positive Level-2 fire-pixel observation exists in this granule, but complete pixel-corner/time evidence is unavailable; this does not establish full swath coverage.",
                    "geometry_source": "LEVEL2_CENTER_ONLY",
                    "granule_id": swath.get("granule_id"),
                }
                record_start = max(start_at, swath_start) if swath_start else start_at
                record_end = min(end_at, swath_end) if swath_end else end_at
            else:
                result = {
                    "opportunity_state": "UNKNOWN", "evidence_source": self.evidence_source,
                    "beta_ret": None, "valid_observed_area_km2": None,
                    "covered_area_km2": None, "cloud_area_fraction": None,
                    "invalid_quality_fraction": None,
                    "reason": (
                        str(swath.get("decode_error")) if swath.get("decode_error") else
                        "No complete corner geolocation and acquisition interval are available for this granule; valid non-detection is not inferred."
                    ),
                    "geometry_source": "UNAVAILABLE",
                    "granule_id": swath.get("granule_id"),
                }
                record_start, record_end = start_at, end_at

            # An instantaneous or incomplete granule cannot be represented as
            # a valid non-detection interval; retain its metadata as UNKNOWN.
            if record_end <= record_start:
                record_start, record_end = start_at, end_at
                if result["opportunity_state"] == "VALID_NO_DETECTION":
                    result["opportunity_state"] = "UNKNOWN"
                    result["reason"] = "The granule has no complete temporal coverage bounds; valid no-detection cannot be asserted for a time interval."
            provenance = {
                "granule_id": result.get("granule_id"),
                "raw_asset_id": swath.get("raw_asset_id"),
                "sha256": swath.get("sha256"),
                "storage_uri": swath.get("storage_uri"),
                "product_id": swath.get("product_id"),
                "collection": swath.get("collection"),
                "geometry_source": result.get("geometry_source"),
                "evidence_source": self.evidence_source,
                "acquisition_start_utc": swath_start.isoformat() if swath_start else None,
                "acquisition_end_utc": swath_end.isoformat() if swath_end else None,
            }
            note = result["reason"]
            if matched_observation is not None and result["opportunity_state"] == "DETECTED":
                note += "; linked to a stored observation row"
            explanation = {
                "plain_language": note, "provenance": provenance,
                "beta_ret": result.get("beta_ret"),
                "evidence_source": self.evidence_source,
            }
            records.append({
                **base,
                "time_window_start": record_start, "time_window_end": record_end,
                "observation_id": matched_observation.get("observation_id") if matched_observation else None,
                "platform": platform, "opportunity_state": result["opportunity_state"],
                "notes": note, "beta_ret": result.get("beta_ret"),
                "valid_observed_area_km2": result.get("valid_observed_area_km2"),
                "covered_area_km2": result.get("covered_area_km2"),
                "cloud_area_fraction": result.get("cloud_area_fraction"),
                "invalid_quality_fraction": result.get("invalid_quality_fraction"),
                "granule_ids_json": json.dumps([result.get("granule_id")]),
                "explanation_json": json.dumps(explanation, sort_keys=True),
            })
        if not records:
            selected = sorted(allowed or seen_platforms) or ["UNKNOWN"]
            for platform in selected:
                records.append({
                    **base, "observation_id": None, "platform": platform,
                    "opportunity_state": "UNKNOWN",
                    "notes": "No local Level-2 granule was available for this platform/time query; no coverage claim is made.",
                    "beta_ret": None, "valid_observed_area_km2": None,
                    "covered_area_km2": None, "cloud_area_fraction": None,
                    "invalid_quality_fraction": None, "granule_ids_json": "[]",
                    "explanation_json": json.dumps({"plain_language": "No Level-2 evidence for the requested time window"}),
                })
        return records


def evaluate_observation_opportunity(
    repository: DatabaseRepository,
    *,
    bbox: str | Sequence[float] | BoundingBox,
    start_date: date | str,
    end_date: date | str,
    sensor: str,
    qc_processing_run_id: int | None = None,
    platform: str | None = None,
    platforms: Sequence[str] | None = None,
    provider: OpportunityProvider | None = None,
    data_source_mode: str = "BOTH",
    method_version: str = DEFAULT_OPPORTUNITY_VERSION,
) -> dict[str, Any]:
    """Evaluate an area/time request; FIRMS zero-row windows always remain UNKNOWN."""
    box = BoundingBox.parse(bbox)
    start_day = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
    end_day = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
    sensor_value = sensor.upper().strip()
    if sensor_value not in {"MODIS", "VIIRS"}:
        raise ValueError("sensor must be MODIS or VIIRS")
    if end_day < start_day:
        raise ValueError("end_date must be on or after start_date")

    start_at = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_at = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    provider_impl = provider or FIRMSDetectionOnlyProvider()
    data_source_mode = data_source_mode.upper()
    if data_source_mode not in {"FIRMS", "LAADS_LEVEL2", "BOTH"}:
        raise ValueError("data_source_mode must be FIRMS, LAADS_LEVEL2, or BOTH")
    configuration = {
        "method": "SANYAM_OBSERVATION_OPPORTUNITY",
        "method_version": method_version,
        "sensor": sensor_value,
        "platform": platform,
        "platforms": list(platforms or ([platform] if platform else [])),
        "bbox": [box.west, box.south, box.east, box.north],
        "spatial_unit_id": box.spatial_unit_id,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "qc_processing_run_id": qc_processing_run_id,
        "data_source_mode": data_source_mode,
        "evidence_source": provider_impl.evidence_source,
    }
    processing_run_id = repository.create_processing_run(
        "OPPORTUNITY", method_version, json.dumps(configuration, sort_keys=True)
    )
    try:
        input_rows = repository.fetch_observations_for_opportunity(
            sensor=sensor_value,
            west=box.west,
            south=box.south,
            east=box.east,
            north=box.north,
            start_at=start_at,
            end_at=end_at,
            qc_processing_run_id=qc_processing_run_id,
        )
        selected_platforms = platforms or ([platform] if platform else None)
        if selected_platforms:
            normalized = {_normalize_platform_filter(value) for value in selected_platforms}
            input_rows = [row for row in input_rows if _platform_name(sensor_value, row.get("satellite")) in normalized]
        if data_source_mode == "FIRMS":
            input_rows = [row for row in input_rows if row.get("raw_asset_id") is None]
        elif data_source_mode == "LAADS_LEVEL2":
            input_rows = [row for row in input_rows if row.get("raw_asset_id") is not None]
        records = provider_impl.build_records(
            input_rows,
            processing_run_id=processing_run_id,
            sensor=sensor_value,
            platform_filter=platforms or platform,
            spatial_unit_id=box.spatial_unit_id,
            start_at=start_at,
            end_at=end_at,
        )
        output_count = repository.insert_opportunities(records)
        repository.finish_processing_run(
            processing_run_id,
            status="COMPLETED",
            input_count=len(input_rows),
            output_count=output_count,
        )
    except Exception as exc:
        repository.finish_processing_run(
            processing_run_id,
            status="FAILED",
            input_count=0,
            output_count=0,
            error_message=str(exc),
        )
        raise

    state_counts: dict[str, int] = {}
    for record in records:
        state = record["opportunity_state"]
        state_counts[state] = state_counts.get(state, 0) + 1
    return {
        "processing_run_id": processing_run_id,
        "module_name": "OPPORTUNITY",
        "sensor": sensor_value,
        "platform": platform,
        "spatial_unit_id": box.spatial_unit_id,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "input_count": len(input_rows),
        "output_count": output_count,
        "state_counts": state_counts,
        "evidence_source": provider_impl.evidence_source,
        "status": "COMPLETED",
    }


def query_opportunity_summary(
    repository: DatabaseRepository,
    *,
    bbox: str | Sequence[float] | BoundingBox,
    start_date: date | str,
    end_date: date | str,
    sensor: str,
) -> dict[str, int]:
    box = BoundingBox.parse(bbox)
    start_day = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
    end_day = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
    if end_day < start_day:
        raise ValueError("end_date must be on or after start_date")
    start_at = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_at = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return repository.get_opportunity_summary(
        sensor=sensor.upper().strip(),
        spatial_unit_id=box.spatial_unit_id,
        start_at=start_at,
        end_at=end_at,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SANYAM Module 4 QC and Module 5 Observation Opportunity")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy URL; defaults to DATABASE_URL/SQLite")
    subparsers = parser.add_subparsers(dest="command", required=True)

    qc_parser = subparsers.add_parser("qc", help="Run quality control on unprocessed observations")
    qc_parser.add_argument("--method-version", default=DEFAULT_QC_VERSION)
    qc_parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    qc_parser.add_argument("--reprocess", action="store_true", help="Create new QC results for all observations")

    opportunity_parser = subparsers.add_parser("opportunity", help="Evaluate and store opportunity states")
    _add_opportunity_arguments(opportunity_parser)
    opportunity_parser.add_argument("--qc-processing-run-id", type=int, default=None)
    opportunity_parser.add_argument("--platform", default=None)
    opportunity_parser.add_argument("--platforms", default=None, help="comma-separated platform selection")
    opportunity_parser.add_argument("--evidence-mode", choices=("FIRMS", "LEVEL2"), default="FIRMS")
    opportunity_parser.add_argument("--method-version", default=DEFAULT_OPPORTUNITY_VERSION)

    summary_parser = subparsers.add_parser("summary", help="Query the latest opportunity summary")
    _add_opportunity_arguments(summary_parser)

    return parser


def _add_opportunity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--sensor", choices=("MODIS", "VIIRS"), required=True)
    parser.add_argument("--bbox", required=True, help="west,south,east,north")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, UTC inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD, UTC inclusive")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s | %(levelname)s | %(message)s")
    args = _build_parser().parse_args(argv)
    repository = connect_db(args.database_url)
    try:
        if args.command == "qc":
            result = run_quality_control(
                repository,
                method_version=args.method_version,
                reprocess=args.reprocess,
                batch_size=args.batch_size,
            )
        elif args.command == "opportunity":
            selected_platforms = [item.strip() for item in args.platforms.split(",") if item.strip()] if args.platforms else ([args.platform] if args.platform else None)
            provider = None
            data_source_mode = "FIRMS"
            if args.evidence_mode == "LEVEL2":
                from sanyam_firms_portable.laads_ingest import iter_level2_swaths_for_opportunity
                provider = Level2SwathOpportunityProvider(
                    iter_level2_swaths_for_opportunity(
                        repository, sensor=args.sensor,
                        start_at=datetime.combine(date.fromisoformat(args.start_date), time.min, tzinfo=timezone.utc),
                        end_at=datetime.combine(date.fromisoformat(args.end_date) + timedelta(days=1), time.min, tzinfo=timezone.utc),
                        bbox=[float(v) for v in args.bbox.split(",")],
                        platforms=selected_platforms,
                    )
                )
                data_source_mode = "LAADS_LEVEL2"
            result = evaluate_observation_opportunity(
                repository,
                bbox=args.bbox,
                start_date=args.start_date,
                end_date=args.end_date,
                sensor=args.sensor,
                qc_processing_run_id=args.qc_processing_run_id,
                platform=args.platform,
                platforms=selected_platforms,
                provider=provider,
                data_source_mode=data_source_mode,
                method_version=args.method_version,
            )
        else:
            summary = query_opportunity_summary(
                repository,
                bbox=args.bbox,
                start_date=args.start_date,
                end_date=args.end_date,
                sensor=args.sensor,
            )
            result = {
                "sensor": args.sensor,
                "bbox": args.bbox,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "state_counts": summary,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        LOG.exception("Module execution failed")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
