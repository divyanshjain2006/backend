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
from typing import Any, Mapping, Protocol, Sequence

from db import DatabaseRepository, connect_db

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
        platform_filter: str | None,
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
        platform_filter: str | None,
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
            if platform_filter and platform != _normalize_platform_filter(platform_filter):
                continue
            grouped.setdefault(platform, []).append(row)

        if not grouped:
            platform = _normalize_platform_filter(platform_filter) if platform_filter else "UNKNOWN"
            return [{
                **base,
                "observation_id": None,
                "platform": platform,
                "opportunity_state": "UNKNOWN",
                "notes": FIRMS_UNKNOWN_NOTE,
            }]

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


def evaluate_observation_opportunity(
    repository: DatabaseRepository,
    *,
    bbox: str | Sequence[float] | BoundingBox,
    start_date: date | str,
    end_date: date | str,
    sensor: str,
    qc_processing_run_id: int | None = None,
    platform: str | None = None,
    provider: OpportunityProvider | None = None,
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
    configuration = {
        "method": "SANYAM_OBSERVATION_OPPORTUNITY",
        "method_version": method_version,
        "sensor": sensor_value,
        "platform": platform,
        "bbox": [box.west, box.south, box.east, box.north],
        "spatial_unit_id": box.spatial_unit_id,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "qc_processing_run_id": qc_processing_run_id,
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
        records = provider_impl.build_records(
            input_rows,
            processing_run_id=processing_run_id,
            sensor=sensor_value,
            platform_filter=platform,
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
            result = evaluate_observation_opportunity(
                repository,
                bbox=args.bbox,
                start_date=args.start_date,
                end_date=args.end_date,
                sensor=args.sensor,
                qc_processing_run_id=args.qc_processing_run_id,
                platform=args.platform,
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
