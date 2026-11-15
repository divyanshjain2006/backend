#!/usr/bin/env python3
"""NASA FIRMS CSV ingestion starter for SANYAM.

First milestone: FIRMS only. No LAADS/DAAC access or HDF/NetCDF processing.

Features:
- Reads MAP_KEY from environment or .env
- Queries FIRMS data availability
- Downloads historical Area API CSV in <=5-day chunks
- In auto mode, prefers Standard Processing (SP) and falls back to NRT only
  outside the SP date range
- Supports an SP-only mode for strict historical research
- Preserves each source CSV snapshot and its SHA-256 checksum
- Validates and normalizes common fields while retaining every raw CSV field
- Persists data and ingestion provenance through a portable SQLAlchemy repository
- Deduplicates exact source rows using a stable SHA-256 key

This is an ingestion prototype, not a scientific validation algorithm.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from db import DEFAULT_DATABASE_URL, DatabaseRepository, connect_db

try:
    from dotenv import load_dotenv
except ImportError:  # .env is optional; environment variables also work.
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

BASE_URL = "https://firms.modaps.eosdis.nasa.gov"
DEFAULT_RAW_DIR = Path("data/raw/firms")
MAX_DAYS_PER_REQUEST = 5
DEFAULT_CHUNK_DAYS = 1  # Smaller requests make variable FIRMS transaction costs easier to control.
DEFAULT_TRANSACTION_HEADROOM = 1000  # Stop at 4,000/5,000 by default.
REQUEST_TIMEOUT_SECONDS = 90
USER_AGENT = "SANYAM-FIRMS-Ingestion/0.2 (research prototype)"

# Current FIRMS source identifiers. SP is the default; NRT requires explicit
# selection by the operator so provisional observations are never mixed in by
# accident.
SP_SOURCES = (
    "MODIS_SP",
    "VIIRS_SNPP_SP",
    "VIIRS_NOAA20_SP",
    "VIIRS_NOAA21_SP",
)
NRT_SOURCES = (
    "MODIS_NRT",
    "VIIRS_SNPP_NRT",
    "VIIRS_NOAA20_NRT",
    "VIIRS_NOAA21_NRT",
)
ALLOWED_SOURCES = set(SP_SOURCES) | set(NRT_SOURCES)

# Each family has an SP product and an NRT counterpart. In auto mode, SP wins
# on every date where it is available; NRT is used only for dates not covered
# by SP and is always labelled NRT in the database and run summary.
SOURCE_FAMILIES: dict[str, tuple[str, str]] = {
    "MODIS": ("MODIS_SP", "MODIS_NRT"),
    "VIIRS_SNPP": ("VIIRS_SNPP_SP", "VIIRS_SNPP_NRT"),
    "VIIRS_NOAA20": ("VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT"),
    "VIIRS_NOAA21": ("VIIRS_NOAA21_SP", "VIIRS_NOAA21_NRT"),
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOG = logging.getLogger("sanyam.firms")


class FirmsError(RuntimeError):
    """Raised for FIRMS API, parsing, or ingestion errors."""


class QuotaBudgetReached(FirmsError):
    """Raised when the configured safety margin for the MAP_KEY is reached."""


@dataclass(frozen=True)
class Availability:
    data_id: str
    min_date: date
    max_date: date


@dataclass(frozen=True)
class BBox:
    west: float
    south: float
    east: float
    north: float

    @classmethod
    def parse(cls, value: str) -> "BBox | str":
        if value.strip().lower() == "world":
            return "world"
        parts = value.split(",")
        if len(parts) != 4:
            raise argparse.ArgumentTypeError(
                "bbox must be west,south,east,north or the word 'world'"
            )
        try:
            west, south, east, north = (float(part.strip()) for part in parts)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("bbox values must be numeric") from exc
        if not (-180 <= west < east <= 180):
            raise argparse.ArgumentTypeError(
                "bbox longitude must satisfy -180 <= west < east <= 180; "
                "dateline-crossing boxes are not supported in this starter"
            )
        if not (-90 <= south < north <= 90):
            raise argparse.ArgumentTypeError(
                "bbox latitude must satisfy -90 <= south < north <= 90"
            )
        return cls(west, south, east, north)

    def as_api_value(self) -> str:
        return f"{self.west},{self.south},{self.east},{self.north}"


def require_map_key() -> str:
    key = os.getenv("FIRMS_MAP_KEY", "").strip()
    if not key:
        raise FirmsError(
            "FIRMS_MAP_KEY is missing. Copy .env.example to .env and add your "
            "free NASA FIRMS MAP_KEY, or export FIRMS_MAP_KEY in your shell."
        )
    return key


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"})
    return session


def request_text(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
    retries: int = 3,
) -> str:
    """GET with bounded retries for connection errors and server failures."""
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code == 429:
                raise QuotaBudgetReached(
                    "FIRMS returned HTTP 429 (rate/transaction limit). The request "
                    "was not retried to avoid wasting quota. Check `python firms_ingest.py status`, "
                    "wait for the 10-minute window to reset, then rerun the same date range."
                )
            if response.status_code >= 500:
                if attempt < retries:
                    wait = min(2**attempt, 20)
                    LOG.warning(
                        "FIRMS returned HTTP %s; retrying in %ss",
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue
            response.raise_for_status()
            text = response.text.lstrip("\ufeff").strip()
            if not text:
                return ""
            return text
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            wait = min(2**attempt, 20)
            LOG.warning("Request failed (%s); retrying in %ss", exc, wait)
            time.sleep(wait)
    raise FirmsError(f"FIRMS request failed after retries: {last_error}")


def parse_csv_text(text: str, context: str) -> list[dict[str, str]]:
    """Parse CSV, distinguishing a valid empty response from an API error."""
    if not text:
        return []
    lowered = text.lower()
    if lowered.startswith("too many") or "transaction limit" in lowered:
        raise QuotaBudgetReached(
            f"FIRMS transaction budget was reached for {context}: {text[:500]}. "
            "Wait for the 10-minute window to reset and rerun the same date range."
        )
    if lowered.startswith(("error", "invalid", "unauthorized")):
        raise FirmsError(f"FIRMS returned an error for {context}: {text[:500]}")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise FirmsError(f"FIRMS response for {context} did not contain a CSV header")
    rows: list[dict[str, str]] = []
    for raw in reader:
        # DictReader can produce a None key if a row contains excess columns.
        cleaned = {
            str(key).strip(): (value.strip() if isinstance(value, str) else "")
            for key, value in raw.items()
            if key is not None
        }
        if any(value for value in cleaned.values()):
            rows.append(cleaned)
    return rows


def get_availability(session: requests.Session, map_key: str) -> dict[str, Availability]:
    url = f"{BASE_URL}/api/data_availability/csv/{map_key}/all"
    text = request_text(session, url)
    rows = parse_csv_text(text, "data availability")
    result: dict[str, Availability] = {}
    for row in rows:
        try:
            data_id = row["data_id"].strip().upper()
            result[data_id] = Availability(
                data_id=data_id,
                min_date=date.fromisoformat(row["min_date"][:10]),
                max_date=date.fromisoformat(row["max_date"][:10]),
            )
        except (KeyError, ValueError) as exc:
            raise FirmsError(f"Unexpected data-availability row: {row}") from exc
    if not result:
        raise FirmsError("FIRMS returned no dataset availability rows")
    return result


def get_map_key_status(session: requests.Session, map_key: str) -> dict[str, Any]:
    url = f"{BASE_URL}/mapserver/mapkey_status/"
    text = request_text(session, url, params={"MAP_KEY": map_key})
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise FirmsError(f"Could not parse FIRMS MAP_KEY status: {text[:300]}") from exc


def acq_datetime_utc(row: dict[str, str]) -> datetime:
    """Convert FIRMS acq_date + HHMM acq_time into timezone-aware UTC."""
    day = date.fromisoformat(row["acq_date"][:10])
    raw_time = row["acq_time"].strip()
    if not raw_time.isdigit() or len(raw_time) > 4:
        raise ValueError(f"Invalid acq_time: {raw_time!r}")
    hhmm = raw_time.zfill(4)
    hour, minute = int(hhmm[:2]), int(hhmm[2:])
    if hour > 23 or minute > 59:
        raise ValueError(f"Invalid HHMM acquisition time: {raw_time!r}")
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)


def optional_float(row: dict[str, str], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key, "").strip()
        if value and value.upper() not in {"NA", "N/A", "NULL", "NONE"}:
            try:
                return float(value)
            except ValueError:
                return None
    return None


def stable_row_hash(dataset_id: str, row: dict[str, str]) -> str:
    """Hash dataset ID plus the complete source row (all source columns)."""
    canonical = json.dumps(
        {"dataset_id": dataset_id, "row": {k: row[k] for k in sorted(row)}},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def sensor_from_dataset_id(dataset_id: str) -> str:
    """Return the explicit sensor family encoded by the FIRMS dataset ID."""
    normalized = dataset_id.upper()
    if normalized.startswith("MODIS"):
        return "MODIS"
    if normalized.startswith("VIIRS"):
        return "VIIRS"
    raise ValueError(f"Cannot determine sensor from FIRMS dataset ID: {dataset_id}")


def collection_from_version(source_version: str) -> str | None:
    """Extract collection from FIRMS version metadata without inventing values.

    Examples: 6.1SP -> 6.1, 6.1NRT -> 6.1, 2.0SP -> 2.
    An absent or unrecognized version remains NULL, as required by SKH §6.5.
    """
    if not source_version:
        return None
    match = re.match(r"^\s*(\d+)(?:\.(\d+))?", source_version)
    if not match:
        return None
    major, minor = match.groups()
    if minor is None or set(minor) <= {"0"}:
        return major
    return f"{major}.{minor}"


def normalize_row(dataset_id: str, row: dict[str, str]) -> dict[str, Any]:
    """Map common FIRMS fields while retaining the complete raw source row.

    IMPORTANT: confidence is preserved as raw source text. MODIS and VIIRS
    confidence representations must not be treated as a shared calibrated
    probability. Native fields remain in raw_json.
    """
    required = ("latitude", "longitude", "acq_date", "acq_time")
    missing = [key for key in required if not row.get(key, "").strip()]
    if missing:
        raise ValueError(f"Missing required FIRMS columns/values: {missing}")

    latitude = float(row["latitude"])
    longitude = float(row["longitude"])
    if not -90 <= latitude <= 90:
        raise ValueError(f"Latitude out of range: {latitude}")
    if not -180 <= longitude <= 180:
        raise ValueError(f"Longitude out of range: {longitude}")

    acquired_at = acq_datetime_utc(row)
    frp = optional_float(row, "frp")
    source_version = row.get("version", "").strip()
    sensor = sensor_from_dataset_id(dataset_id)
    return {
        "source_row_hash": stable_row_hash(dataset_id, row),
        "dataset_id": dataset_id,
        "processing_type": "NRT" if dataset_id.endswith("_NRT") else "SP",
        "sensor": sensor,
        "collection": collection_from_version(source_version),
        "acquired_at": acquired_at,
        "latitude": latitude,
        "longitude": longitude,
        "satellite": row.get("satellite", "").strip(),
        "instrument": row.get("instrument", "").strip(),
        "confidence_raw": row.get("confidence", "").strip(),
        "frp_mw": frp,
        "brightness_k": optional_float(row, "brightness"),
        "bright_ti4_k": optional_float(row, "bright_ti4"),
        "bright_ti5_k": optional_float(row, "bright_ti5"),
        "scan": optional_float(row, "scan"),
        "track": optional_float(row, "track"),
        "daynight": row.get("daynight", "").strip(),
        "source_version": source_version,
        "footprint_source": "FIRMS_POINT_SCAN_TRACK",
        "opportunity_state": "UNKNOWN",
        "raw_json": json.dumps(row, sort_keys=True, separators=(",", ":")),
    }


def normalize_source_rows(
    dataset_id: str, source_rows: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], int]:
    """Validate source rows and return normalized records plus rejected count."""
    valid: list[dict[str, Any]] = []
    rejected = 0
    for row in source_rows:
        try:
            valid.append(normalize_row(dataset_id, row))
        except (ValueError, KeyError, TypeError) as exc:
            rejected += 1
            LOG.warning("Rejected one source row from %s: %s", dataset_id, exc)
    return valid, rejected


def save_source_snapshot(raw_dir: Path, dataset_id: str, start: date, end: date, text: str) -> tuple[Path, str]:
    folder = raw_dir / dataset_id
    folder.mkdir(parents=True, exist_ok=True)
    data = text.encode("utf-8")
    checksum = hashlib.sha256(data).hexdigest()
    # Include the checksum so a later upstream reprocessing cannot silently
    # overwrite the earlier source response for the same date interval.
    path = folder / f"{start.isoformat()}_{end.isoformat()}_{checksum[:12]}.csv"
    if not path.exists():
        path.write_bytes(data)
    return path, checksum


def request_area_csv(
    session: requests.Session,
    map_key: str,
    dataset_id: str,
    bbox: BBox | str,
    chunk_start: date,
    days: int,
) -> str:
    if not 1 <= days <= MAX_DAYS_PER_REQUEST:
        raise ValueError(f"FIRMS Area API day range must be 1..{MAX_DAYS_PER_REQUEST}")
    area = "world" if bbox == "world" else bbox.as_api_value()
    url = (
        f"{BASE_URL}/api/area/csv/{map_key}/{dataset_id}/"
        f"{area}/{days}/{chunk_start.isoformat()}"
    )
    return request_text(session, url)


def date_chunks(start: date, end: date, max_days: int = MAX_DAYS_PER_REQUEST) -> Iterable[tuple[date, int]]:
    current = start
    while current <= end:
        days = min(max_days, (end - current).days + 1)
        yield current, days
        current += timedelta(days=days)


def intersect_dates(
    requested_start: date,
    requested_end: date,
    availability: Availability,
) -> tuple[date, date] | None:
    start = max(requested_start, availability.min_date)
    end = min(requested_end, availability.max_date)
    return (start, end) if start <= end else None


def build_auto_plan(
    requested_start: date,
    requested_end: date,
    availability: dict[str, Availability],
    *,
    include_nrt_fallback: bool = True,
    families: Iterable[str] | None = None,
) -> list[tuple[str, date, date]]:
    """Build non-overlapping date windows, preferring SP over NRT.

    FIRMS data_availability supplies min/max dates, not a per-day inventory.
    This planner selects SP wherever its published range covers a day. NRT is
    only a fallback outside the SP range, if enabled and listed as available.
    Any date outside both ranges is omitted and reported by the caller.
    """
    if requested_end < requested_start:
        raise ValueError("requested_end must be on or after requested_start")

    family_names = list(families) if families is not None else list(SOURCE_FAMILIES)
    unknown = sorted(set(family_names) - set(SOURCE_FAMILIES))
    if unknown:
        raise ValueError(f"Unknown sensor family/families: {', '.join(unknown)}")

    plan: list[tuple[str, date, date]] = []
    for family in family_names:
        sp_id, nrt_id = SOURCE_FAMILIES[family]
        sp = availability.get(sp_id)
        nrt = availability.get(nrt_id) if include_nrt_fallback else None
        active_source: str | None = None
        active_start: date | None = None
        previous_day: date | None = None

        current = requested_start
        while current <= requested_end:
            chosen: str | None = None
            if sp and sp.min_date <= current <= sp.max_date:
                chosen = sp_id
            elif nrt and nrt.min_date <= current <= nrt.max_date:
                chosen = nrt_id

            if chosen != active_source:
                if active_source is not None and active_start is not None and previous_day is not None:
                    plan.append((active_source, active_start, previous_day))
                active_source = chosen
                active_start = current if chosen is not None else None
            previous_day = current
            current += timedelta(days=1)

        if active_source is not None and active_start is not None and previous_day is not None:
            plan.append((active_source, active_start, previous_day))

    return plan


def status_transaction_counts(status: dict[str, Any]) -> tuple[int, int]:
    """Read current and limit counts from the FIRMS MAP_KEY status response."""
    try:
        current = int(status["current_transactions"])
        limit = int(status.get("transaction_limit", 5000))
    except (KeyError, TypeError, ValueError) as exc:
        raise FirmsError(f"Unexpected MAP_KEY status response: {status}") from exc
    if current < 0 or limit <= 0:
        raise FirmsError(f"Invalid MAP_KEY transaction counts: {status}")
    return current, limit


def check_transaction_headroom(
    session: requests.Session,
    map_key: str,
    reserve: int,
) -> tuple[int, int]:
    """Fail closed when current usage reaches limit minus reserved headroom."""
    status = get_map_key_status(session, map_key)
    current, limit = status_transaction_counts(status)
    if reserve < 1 or reserve >= limit:
        raise FirmsError(f"Transaction reserve must be between 1 and {limit - 1}")
    safe_ceiling = limit - reserve
    if current >= safe_ceiling:
        raise QuotaBudgetReached(
            f"MAP_KEY usage is {current}/{limit}; stopping at the configured "
            f"safety ceiling of {safe_ceiling}. Wait for the 10-minute window "
            "to reset, then resume the same date range."
        )
    return current, limit


def ingest_dataset(
    *,
    session: requests.Session,
    db: DatabaseRepository,
    map_key: str,
    dataset_id: str,
    start_date: date,
    end_date: date,
    bbox: BBox | str,
    raw_dir: Path,
    keep_snapshots: bool = True,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    transaction_reserve: int = DEFAULT_TRANSACTION_HEADROOM,
    resume: bool = False,
) -> dict[str, Any]:
    totals: dict[str, Any] = {
        "requests": 0, "fetched": 0, "inserted": 0, "duplicates": 0,
        "rejected": 0, "skipped_completed": 0, "paused_quota": False,
    }
    bbox_value = "world" if bbox == "world" else bbox.as_api_value()

    for chunk_start, days in date_chunks(start_date, end_date, max_days=chunk_days):
        chunk_end = chunk_start + timedelta(days=days - 1)
        if resume and db.has_completed_chunk(dataset_id, chunk_start, chunk_end, bbox_value):
            totals["skipped_completed"] += 1
            LOG.info(
                "Resume: skipping already-completed %s %s..%s for %s",
                dataset_id, chunk_start, chunk_end, bbox_value,
            )
            continue

        # Check the live rolling-window counter before each expensive data call.
        # The reserve protects capacity for variable request costs and other API use.
        try:
            current_usage, transaction_limit = check_transaction_headroom(
                session, map_key, transaction_reserve
            )
        except QuotaBudgetReached as exc:
            LOG.warning("%s", exc)
            totals["paused_quota"] = True
            break
        LOG.info(
            "MAP_KEY usage before %s request: %d/%d transactions",
            dataset_id, current_usage, transaction_limit,
        )

        run_id = db.create_run(dataset_id, chunk_start, chunk_end, bbox_value)
        try:
            text = request_area_csv(session, map_key, dataset_id, bbox, chunk_start, days)
            totals["requests"] += 1
            rows = parse_csv_text(text, f"{dataset_id} {chunk_start}..{chunk_end}")
            source_path: str | None = None
            source_sha: str | None = None
            if keep_snapshots and text:
                path, source_sha = save_source_snapshot(
                    raw_dir, dataset_id, chunk_start, chunk_end, text
                )
                source_path = str(path)

            normalized_rows, rejected = normalize_source_rows(dataset_id, rows)
            inserted, duplicates = db.insert_observations(run_id, normalized_rows)
            db.finish_run(
                run_id, status="COMPLETED", fetched=len(rows),
                inserted=inserted, duplicates=duplicates, rejected=rejected,
                source_path=source_path, source_sha256=source_sha,
            )
            totals["fetched"] += len(rows)
            totals["inserted"] += inserted
            totals["duplicates"] += duplicates
            totals["rejected"] += rejected
            LOG.info(
                "%s %s..%s: fetched=%d inserted=%d duplicates=%d rejected=%d",
                dataset_id, chunk_start, chunk_end, len(rows), inserted,
                duplicates, rejected,
            )
        except QuotaBudgetReached as exc:
            db.finish_run(run_id, status="PAUSED_QUOTA", error=str(exc))
            LOG.warning("%s", exc)
            totals["paused_quota"] = True
            break
        except Exception as exc:
            db.finish_run(run_id, status="FAILED", error=str(exc))
            raise
    return totals


def cmd_availability(args: argparse.Namespace) -> int:
    map_key = require_map_key()
    session = make_session()
    try:
        availability = get_availability(session, map_key)
        print_availability(availability)
        return 0
    finally:
        session.close()


def cmd_status(args: argparse.Namespace) -> int:
    map_key = require_map_key()
    session = make_session()
    try:
        status = get_map_key_status(session, map_key)
        print(json.dumps(status, indent=2))
        return 0
    finally:
        session.close()


def print_availability(availability: dict[str, Availability]) -> None:
    print("FIRMS dataset availability (dates are UTC/GMT):")
    print(f"{'DATASET':<24} {'MIN DATE':<12} {'MAX DATE':<12}")
    for data_id in sorted(availability):
        item = availability[data_id]
        print(f"{data_id:<24} {item.min_date!s:<12} {item.max_date!s:<12}")


def cmd_ingest(args: argparse.Namespace) -> int:
    map_key = require_map_key()
    requested_start = date.fromisoformat(args.start_date)
    requested_end = date.fromisoformat(args.end_date)
    if requested_end < requested_start:
        raise FirmsError("--end-date must be on or after --start-date")

    session = make_session()
    try:
        availability = get_availability(session, map_key)
        print_availability(availability)

        # Manual source selection remains available for repeatable experiments.
        if args.sources:
            sources = args.sources
            for source in sources:
                if source not in ALLOWED_SOURCES:
                    raise FirmsError(
                        f"Unsupported source {source!r}. Allowed: {', '.join(sorted(ALLOWED_SOURCES))}"
                    )
                if source.endswith("_NRT") and not args.allow_nrt:
                    raise FirmsError(
                        f"{source} is NRT. Re-run with --allow-nrt to explicitly permit provisional data."
                    )
            plan: list[tuple[str, date, date]] = []
            for source in sources:
                available = availability.get(source)
                if available is None:
                    LOG.warning("Skipping %s: FIRMS did not list this dataset", source)
                    continue
                overlap = intersect_dates(requested_start, requested_end, available)
                if overlap is not None:
                    plan.append((source, overlap[0], overlap[1]))
                else:
                    LOG.info("Skipping %s: no overlap with requested date interval", source)
        else:
            families = args.families or list(SOURCE_FAMILIES)
            plan = build_auto_plan(
                requested_start,
                requested_end,
                availability,
                include_nrt_fallback=(args.mode == "auto"),
                families=families,
            )

        if not plan:
            print("No source/date windows are available for the requested interval.")
            return 0

        print("\nPlanned FIRMS source windows (auto mode prefers SP; NRT is used only where SP is unavailable):")
        for data_id, start, end in plan:
            kind = "PROVISIONAL NRT" if data_id.endswith("_NRT") else "STANDARD SP"
            print(f"  {data_id:<22} {start} .. {end}  [{kind}]")

        database_url = args.database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL
        if args.db:
            if args.database_url:
                raise FirmsError("Use either --db or --database-url, not both.")
            database_url = args.db
        db = connect_db(database_url)
        try:
            print(f"\nDatabase: {db.display_target}")
            print(f"Bounding box: {args.bbox if isinstance(args.bbox, str) else args.bbox.as_api_value()}")
            print(f"Requested dates: {requested_start} through {requested_end} (inclusive)")
            print(f"Chunk size: {args.chunk_days} day(s); transaction reserve: {args.transaction_reserve}")

            summaries: dict[str, Any] = {}
            for source, actual_start, actual_end in plan:
                totals = ingest_dataset(
                    session=session,
                    db=db,
                    map_key=map_key,
                    dataset_id=source,
                    start_date=actual_start,
                    end_date=actual_end,
                    bbox=args.bbox,
                    raw_dir=Path(args.raw_dir),
                    keep_snapshots=not args.no_snapshots,
                    chunk_days=args.chunk_days,
                    transaction_reserve=args.transaction_reserve,
                    resume=args.resume,
                )
                summaries[f"{source}:{actual_start}:{actual_end}"] = {
                    "status": "PAUSED_QUOTA" if totals["paused_quota"] else "COMPLETED",
                    "actual_start": actual_start.isoformat(),
                    "actual_end": actual_end.isoformat(),
                    **totals,
                }
                if totals["paused_quota"]:
                    LOG.warning("Stopping remaining datasets to protect the FIRMS MAP_KEY quota.")
                    break

            try:
                final_status = get_map_key_status(session, map_key)
                current, limit = status_transaction_counts(final_status)
                summaries["map_key_usage"] = {
                    "current_transactions": current,
                    "transaction_limit": limit,
                    "remaining_reported": max(0, limit - current),
                    "note": "FIRMS rolling 10-minute window; other clients using this key also count.",
                }
            except FirmsError as exc:
                summaries["map_key_usage"] = {"status": "UNAVAILABLE", "message": str(exc)}

            print("\nIngestion summary:")
            print(json.dumps(summaries, indent=2))
            return 0
        finally:
            db.close()
    finally:
        session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest NASA FIRMS observations into SQLite or PostgreSQL with SP-first/NRT-fallback selection."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    availability_parser = subparsers.add_parser(
        "availability", help="List FIRMS dataset date availability"
    )
    availability_parser.set_defaults(func=cmd_availability)

    status_parser = subparsers.add_parser(
        "status", help="Check FIRMS MAP_KEY transaction status"
    )
    status_parser.set_defaults(func=cmd_status)

    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest a date range using automatically discovered FIRMS SP/NRT availability"
    )
    ingest_parser.add_argument("--start-date", required=True, help="YYYY-MM-DD (UTC)")
    ingest_parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (UTC, inclusive)")
    ingest_parser.add_argument(
        "--bbox", required=True, type=BBox.parse,
        help="west,south,east,north (e.g. 73.0,26.0,78.0,30.0) or world",
    )
    ingest_parser.add_argument(
        "--source", dest="sources", action="append", choices=sorted(ALLOWED_SOURCES),
        help="Optional explicit dataset ID; repeat to select multiple. Manual NRT requires --allow-nrt.",
    )
    ingest_parser.add_argument(
        "--mode", choices=("auto", "sp-only"), default="auto",
        help="auto prefers SP and falls back to NRT only on dates without SP; sp-only never uses NRT (default: auto).",
    )
    ingest_parser.add_argument(
        "--family", dest="families", action="append", choices=sorted(SOURCE_FAMILIES),
        help="Sensor family for automatic mode; repeat to select multiple. Defaults to all listed families.",
    )
    ingest_parser.add_argument(
        "--allow-nrt", action="store_true",
        help="Required safety switch before ingesting any *_NRT source.",
    )
    ingest_parser.add_argument(
        "--chunk-days", type=int, choices=range(1, MAX_DAYS_PER_REQUEST + 1),
        default=DEFAULT_CHUNK_DAYS,
        help="Days per Area API request (1..5). Default 1 is safer for large/global results.",
    )
    ingest_parser.add_argument(
        "--transaction-reserve", type=int, default=DEFAULT_TRANSACTION_HEADROOM,
        help="Stop starting new data requests when MAP_KEY usage reaches limit minus this reserve (default 1000).",
    )
    ingest_parser.add_argument(
        "--resume", action="store_true",
        help="Skip exact dataset/date/bbox chunks already marked COMPLETED. Use after a quota pause; omit to refresh source data.",
    )
    ingest_parser.add_argument(
        "--database-url", default=None,
        help="SQLAlchemy URL (default: DATABASE_URL or sqlite:///data/sanyam_firms.sqlite3). "
             "PostgreSQL example: postgresql+psycopg://user:password@host:5432/dbname",
    )
    ingest_parser.add_argument(
        "--db", default=None,
        help="Deprecated SQLite path shorthand; use DATABASE_URL or --database-url",
    )
    ingest_parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR), help="Source CSV snapshot directory")
    ingest_parser.add_argument(
        "--no-snapshots", action="store_true",
        help="Do not save original CSV response snapshots (not recommended for research).",
    )
    ingest_parser.set_defaults(func=cmd_ingest)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (FirmsError, ValueError) as exc:
        LOG.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        LOG.warning("Interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
