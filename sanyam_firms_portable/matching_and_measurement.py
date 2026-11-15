#!/usr/bin/env python3
"""SANYAM Module 6: matching, FRP diagnostics, and common-grid activity analysis.

Implements the FIRMS-CSV stage of SKH Master Documentation §§6.8, 6.10,
7.2–7.7. Original observation values are never modified. All persistence uses
DatabaseRepository from db.py (SQLite or PostgreSQL).

Scientific boundaries:
- Baseline matching is a spherical point-centroid screen, not fire-event identity.
- FIRMS scan/track rectangles are explicitly approximate; they are not Level-2
  pixel-corner footprints.
- No FRP adjustment is made unless an explicit, separately validated
  observation-operator implementation is supplied.
- The detection-limit numbers are documented reference-point diagnostics,
  scaled linearly by estimated pixel-area ratio. They are not the full Sofiev &
  Kouznetsov observation operator and are not universal detection thresholds.
- FIRMS-only common-grid valid_area_fraction remains NULL and zero-detection
  cells remain UNKNOWN, never inferred to be inactive.
"""
from __future__ import annotations

import argparse
import bisect
import json
import hashlib
import logging
import math
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from pyproj import CRS, Transformer
from shapely import make_valid
from shapely.geometry import LineString, Point, Polygon, box, mapping, shape
from shapely.strtree import STRtree
from shapely.ops import transform as transform_geometry
from shapely.wkt import loads as load_wkt

from db import DatabaseRepository, connect_db
from qc_and_opportunity import BoundingBox

try:
    from dotenv import load_dotenv
except ImportError:  # .env is optional; environment variables also work.
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

LOG = logging.getLogger("sanyam.matching_measurement")
EARTH_RADIUS_KM = 6371.0088
DEFAULT_BASELINE_DISTANCE_KM = 2.0
DEFAULT_TIME_DIFFERENCE_MINUTES = 30.0
DEFAULT_BOUNDARY_DISTANCE_KM = 0.25
DEFAULT_FALLBACK_CENTROID_DISTANCE_KM = 2.0
DEFAULT_GRID_RESOLUTION_KM = 1.0
METHOD_VERSION = "1.0.0"
FIRMS_GEOMETRY_SOURCE = "FIRMS_APPROX_SCAN_TRACK"
POINT_FALLBACK_GEOMETRY_SOURCE = "POINT_CENTROID_FALLBACK"
LEVEL2_GEOMETRY_SOURCE = "LEVEL2_PIXEL_CORNERS"
FRP_DIAGNOSTIC_VERSION = "reference-detection-limit-area-scaled/1.0"
FIRMS_OPPORTUNITY_NOTE = (
    "FIRMS point-only detections do not supply complete swath/cloud masks; "
    "valid observed area is unknown."
)


@dataclass(frozen=True)
class EqualAreaContext:
    bbox: BoundingBox
    crs: CRS
    crs_name: str
    forward: Transformer
    inverse: Transformer
    study_polygon: Polygon


def _parse_date(value: date | str) -> date:
    return date.fromisoformat(value) if isinstance(value, str) else value


def _date_bounds(start_date: date | str, end_date: date | str) -> tuple[datetime, datetime, date, date]:
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)
    if end_day < start_day:
        raise ValueError("end_date must be on or after start_date")
    start_at = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_at = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return start_at, end_at, start_day, end_day


TEMPORAL_WINDOW_PRESETS = (5, 15, 30, 60, 120)


def resolve_temporal_window(
    *, window_preset: int | str | None = None,
    temporal_window_minutes: float | None = None,
    legacy_default: float = DEFAULT_TIME_DIFFERENCE_MINUTES,
) -> tuple[float, str, int | None]:
    """Resolve a named preset or 1..360 minute custom window."""
    if window_preset is not None and temporal_window_minutes is not None:
        raise ValueError("choose either window_preset or temporal_window_minutes, not both")
    if window_preset is not None:
        try:
            preset = int(window_preset)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"window_preset must be one of {TEMPORAL_WINDOW_PRESETS}") from exc
        if preset not in TEMPORAL_WINDOW_PRESETS:
            raise ValueError(f"window_preset must be one of {TEMPORAL_WINDOW_PRESETS}")
        return float(preset), "PRESET", preset
    if temporal_window_minutes is not None:
        custom = float(temporal_window_minutes)
        if not math.isfinite(custom) or not 1.0 <= custom <= 360.0:
            raise ValueError("custom temporal window must be between 1 and 360 minutes")
        return custom, "CUSTOM", None
    legacy = float(legacy_default)
    if not math.isfinite(legacy) or legacy < 0 or legacy > 360:
        raise ValueError("max_time_difference_minutes must be between 0 and 360")
    return legacy, "LEGACY_DEFAULT_OR_FLAG", None


def _platform_diagnostics(platforms: Sequence[str] | None, start_at: datetime | None, end_at: datetime | None) -> list[dict[str, str]]:
    if not platforms or start_at is None or end_at is None:
        return []
    from laads_ingest import platform_availability_diagnostics
    return platform_availability_diagnostics(platforms, start_at, end_at)


def _normalise_platforms(platforms: Sequence[str] | None) -> set[str] | None:
    if not platforms:
        return None
    from laads_ingest import canonical_platform
    return {canonical_platform(value) for value in platforms}


def _row_platform(row: Mapping[str, Any]) -> str:
    from qc_and_opportunity import _platform_name
    return _platform_name(str(row.get("sensor") or ""), row.get("satellite"))


def _within_window(value: Any, start_at: datetime | None, end_at: datetime | None) -> bool:
    if start_at is None or end_at is None:
        return True
    instant = _utc(value)
    return start_at <= instant < end_at


def _pair_touches_query_window(
    a: Mapping[str, Any], b: Mapping[str, Any],
    start_at: datetime | None, end_at: datetime | None,
) -> bool:
    return _within_window(a["acquired_at"], start_at, end_at) or _within_window(b["acquired_at"], start_at, end_at)


def _run_cache_key(configuration: Mapping[str, Any]) -> str:
    stable = {
        "selected_platforms": sorted(configuration.get("selected_platforms") or []),
        "temporal_window_minutes": configuration.get("max_time_difference_minutes"),
        "spatial_method": configuration.get("method"),
        "grid_resolution_km": configuration.get("grid_resolution_km"),
        "data_source_mode": configuration.get("data_source_mode", "BOTH"),
    }
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def haversine_distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Spherical Haversine centre distance in kilometres (R=6371.0088 km)."""
    values = (lon1, lat1, lon2, lat2)
    if not all(math.isfinite(float(v)) for v in values):
        raise ValueError("Haversine coordinates must be finite")
    if not (-180 <= lon1 <= 180 and -180 <= lon2 <= 180):
        raise ValueError("Longitude must be within -180..180")
    if not (-90 <= lat1 <= 90 and -90 <= lat2 <= 90):
        raise ValueError("Latitude must be within -90..90")
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    # Normalize the longitude delta across the antimeridian.
    dlon_deg = (lon2 - lon1 + 180.0) % 360.0 - 180.0
    dlambda = math.radians(dlon_deg)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    a = min(1.0, max(0.0, a))
    return 2.0 * EARTH_RADIUS_KM * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _densified_bbox_ring(bbox_value: BoundingBox, segments_per_edge: int = 24) -> list[tuple[float, float]]:
    west, south, east, north = bbox_value.west, bbox_value.south, bbox_value.east, bbox_value.north
    points: list[tuple[float, float]] = []
    for i in range(segments_per_edge):
        f = i / segments_per_edge
        points.append((west + (east - west) * f, south))
    for i in range(segments_per_edge):
        f = i / segments_per_edge
        points.append((east, south + (north - south) * f))
    for i in range(segments_per_edge):
        f = i / segments_per_edge
        points.append((east - (east - west) * f, north))
    for i in range(segments_per_edge):
        f = i / segments_per_edge
        points.append((west, north - (north - south) * f))
    points.append(points[0])
    return points


def build_equal_area_context(
    bbox: str | Sequence[float] | BoundingBox,
) -> EqualAreaContext:
    """Build a local Lambert Azimuthal Equal-Area projection for the study bbox."""
    parsed = BoundingBox.parse(bbox)
    lon0 = (parsed.west + parsed.east) / 2.0
    lat0 = (parsed.south + parsed.north) / 2.0
    crs = CRS.from_proj4(
        f"+proj=laea +lat_0={lat0:.10f} +lon_0={lon0:.10f} "
        "+datum=WGS84 +units=m +no_defs +type=crs"
    )
    forward = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    inverse = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    projected_ring = [forward.transform(lon, lat) for lon, lat in _densified_bbox_ring(parsed)]
    study_polygon = Polygon(projected_ring)
    if not study_polygon.is_valid:
        study_polygon = make_valid(study_polygon)
    if study_polygon.is_empty or study_polygon.area <= 0:
        raise ValueError("Study bbox cannot be represented as a valid projected polygon")
    return EqualAreaContext(
        bbox=parsed,
        crs=crs,
        crs_name=crs.to_string(),
        forward=forward,
        inverse=inverse,
        study_polygon=study_polygon,
    )


def _derive_bbox(observations: Sequence[Mapping[str, Any]]) -> BoundingBox:
    if not observations:
        return BoundingBox(-1.0, -1.0, 1.0, 1.0)
    lons = [float(row["longitude"]) for row in observations]
    lats = [float(row["latitude"]) for row in observations]
    west, east = min(lons), max(lons)
    south, north = min(lats), max(lats)
    if west == east:
        west, east = max(-180.0, west - 0.05), min(180.0, east + 0.05)
    if south == north:
        south, north = max(-90.0, south - 0.05), min(90.0, north + 0.05)
    return BoundingBox(west, south, east, north)


def _positive_scan_track(row: Mapping[str, Any]) -> tuple[float, float] | None:
    try:
        scan = float(row.get("scan"))
        track = float(row.get("track"))
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(scan) and math.isfinite(track) and scan > 0 and track > 0):
        return None
    # FIRMS scan/track dimensions are represented in kilometres. Reject
    # physically implausible values rather than constructing giant polygons.
    if scan > 100.0 or track > 100.0:
        return None
    return scan, track


def approximate_firms_footprint(
    observation: Mapping[str, Any], context: EqualAreaContext,
) -> Polygon | None:
    """Reconstruct an explicitly approximate axis-aligned rectangle in LAEA metres.

    FIRMS does not provide Level-2 pixel corner coordinates. Scan is used as
    across-track width and track as along-track height; absent orbit-heading
    information means this is a proxy geometry, not a physical corner polygon.
    """
    dimensions = _positive_scan_track(observation)
    if dimensions is None:
        return None
    scan_km, track_km = dimensions
    x, y = context.forward.transform(float(observation["longitude"]), float(observation["latitude"]))
    width_m, height_m = scan_km * 1000.0, track_km * 1000.0
    polygon = box(x - width_m / 2.0, y - height_m / 2.0, x + width_m / 2.0, y + height_m / 2.0)
    if not polygon.is_valid:
        polygon = make_valid(polygon)
    return polygon if isinstance(polygon, Polygon) and not polygon.is_empty else None


class FootprintProvider(Protocol):
    """Provider hook for FIRMS proxy footprints and future Level-2 pixel corners."""

    geometry_source: str

    def build_polygon(
        self, observation: Mapping[str, Any], context: EqualAreaContext,
    ) -> Polygon | None:
        """Return a valid footprint polygon in the supplied local equal-area CRS."""


class FIRMSScanTrackFootprintProvider:
    """FIRMS-only approximate proxy; never represents true Level-2 corners."""

    geometry_source = FIRMS_GEOMETRY_SOURCE

    def build_polygon(
        self, observation: Mapping[str, Any], context: EqualAreaContext,
    ) -> Polygon | None:
        return approximate_firms_footprint(observation, context)


class CompositeFootprintProvider:
    """Prefer true LAADS pixel-corner WKT; explicitly fall back to FIRMS proxy."""

    geometry_source = "MIXED_LEVEL2_AND_FIRMS_GEOMETRY"

    def build_polygon(self, observation: Mapping[str, Any], context: EqualAreaContext) -> Polygon | None:
        wkt = observation.get("footprint_wkt")
        if wkt:
            try:
                geographic = load_wkt(str(wkt))
                projected = transform_geometry(context.forward.transform, geographic)
                if projected.is_valid and projected.area > 0:
                    return projected if isinstance(projected, Polygon) else projected.convex_hull
            except Exception:
                LOG.warning("Invalid stored Level-2 footprint WKT for observation_id=%s", observation.get("observation_id"))
        return approximate_firms_footprint(observation, context)

    def source_for(self, row: Mapping[str, Any]) -> str:
        if row.get("footprint_wkt") and row.get("footprint_source") == LEVEL2_GEOMETRY_SOURCE:
            return LEVEL2_GEOMETRY_SOURCE
        if _positive_scan_track(row):
            return FIRMS_GEOMETRY_SOURCE
        return POINT_FALLBACK_GEOMETRY_SOURCE


# The provider only reports LEVEL2_PIXEL_CORNERS when explicit corner geometry
# was ingested; it does not infer corners from sparse fire-pixel centres.


def footprint_metrics(
    modis_polygon: Polygon,
    viirs_polygon: Polygon,
) -> dict[str, float]:
    """Calculate footprint metrics in a projected equal-area/metre CRS."""
    if modis_polygon.is_empty or viirs_polygon.is_empty:
        raise ValueError("Footprint polygons must be non-empty")
    a = modis_polygon if modis_polygon.is_valid else make_valid(modis_polygon)
    b = viirs_polygon if viirs_polygon.is_valid else make_valid(viirs_polygon)
    area_a, area_b = a.area, b.area
    if area_a <= 0 or area_b <= 0:
        raise ValueError("Footprint polygons must have positive area")
    intersection_area = a.intersection(b).area
    union_area = area_a + area_b - intersection_area
    return {
        "intersection_area_km2": intersection_area / 1_000_000.0,
        "iou": intersection_area / union_area if union_area > 0 else 0.0,
        "smaller_footprint_overlap": intersection_area / min(area_a, area_b),
        "boundary_distance_km": a.boundary.distance(b.boundary) / 1000.0,
    }


def _filter_observations(
    repository: DatabaseRepository,
    *,
    start_at: datetime | None,
    end_at: datetime | None,
    bbox: BoundingBox | None,
    qc_processing_run_id: int | None,
    platforms: Sequence[str] | None = None,
    data_source_mode: str = "BOTH",
    time_padding_minutes: float = 0.0,
) -> list[dict[str, Any]]:
    fetch_start = start_at - timedelta(minutes=time_padding_minutes) if start_at is not None else None
    fetch_end = end_at + timedelta(minutes=time_padding_minutes) if end_at is not None else None
    kwargs: dict[str, Any] = {
        "sensors": ("MODIS", "VIIRS"),
        "start_at": fetch_start,
        "end_at": fetch_end,
        "qc_processing_run_id": qc_processing_run_id,
        "require_matching_suitability": True,
        "data_source_mode": data_source_mode,
    }
    if bbox is not None:
        kwargs.update(west=bbox.west, south=bbox.south, east=bbox.east, north=bbox.north)
    rows = repository.fetch_scientific_observations(**kwargs)
    selected_platforms = _normalise_platforms(platforms)
    result = []
    for row in rows:
        if row.get("sensor") not in {"MODIS", "VIIRS"}:
            continue
        if selected_platforms and _row_platform(row) not in selected_platforms:
            continue
        result.append(dict(row))
    return result


def _pair_time_difference_minutes(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    return abs((_utc(a["acquired_at"]) - _utc(b["acquired_at"])).total_seconds()) / 60.0


def _partition_by_sensor(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    modis, viirs = [], []
    for row in rows:
        if row.get("sensor") == "MODIS":
            modis.append(dict(row))
        elif row.get("sensor") == "VIIRS":
            viirs.append(dict(row))
    return modis, viirs


def _candidate_graph_components(
    edges: Sequence[Mapping[str, Any]],
) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], str], dict[str, tuple[int, int, int]]]:
    """Return node degrees, stable component IDs, and component shape counts."""
    adjacency: dict[tuple[str, int], set[tuple[str, int]]] = defaultdict(set)
    for edge in edges:
        m_id, v_id = edge.get("modis_observation_id"), edge.get("viirs_observation_id")
        if m_id is None or v_id is None:
            continue
        m_node, v_node = ("MODIS", int(m_id)), ("VIIRS", int(v_id))
        adjacency[m_node].add(v_node)
        adjacency[v_node].add(m_node)
    degrees = {node: len(neighbours) for node, neighbours in adjacency.items()}
    component_ids: dict[tuple[str, int], str] = {}
    component_shapes: dict[str, tuple[int, int, int]] = {}
    visited: set[tuple[str, int]] = set()
    for start in sorted(adjacency, key=lambda item: (item[0], item[1])):
        if start in visited:
            continue
        queue = deque([start])
        members: list[tuple[str, int]] = []
        visited.add(start)
        while queue:
            current = queue.popleft()
            members.append(current)
            for neighbour in adjacency[current]:
                if neighbour not in visited:
                    visited.add(neighbour)
                    queue.append(neighbour)
        m_count = sum(1 for sensor, _ in members if sensor == "MODIS")
        v_count = sum(1 for sensor, _ in members if sensor == "VIIRS")
        edge_count = sum(degrees[node] for node in members) // 2
        canonical_nodes = ",".join(f"{sensor}:{observation_id}" for sensor, observation_id in sorted(members))
        component_id = "C" + hashlib.sha256(canonical_nodes.encode("utf-8")).hexdigest()[:16]
        component_shapes[component_id] = (m_count, v_count, edge_count)
        for node in members:
            component_ids[node] = component_id
    return degrees, component_ids, component_shapes


def _classify_edges(edges: list[dict[str, Any]]) -> None:
    degree_by_node, component_ids, component_shapes = _candidate_graph_components(edges)
    for edge in edges:
        m_id, v_id = int(edge["modis_observation_id"]), int(edge["viirs_observation_id"])
        m_degree = degree_by_node.get(("MODIS", m_id), 0)
        v_degree = degree_by_node.get(("VIIRS", v_id), 0)
        component_id = component_ids.get(("MODIS", m_id))
        m_count, v_count, edge_count = component_shapes.get(component_id, (1, 1, 1))
        if m_count > 1 and v_count > 1 and edge_count > 1:
            status = "AMBIGUOUS_GROUP"
        elif m_degree > 1 or v_degree > 1:
            status = "MULTIPLE_CANDIDATES"
        else:
            status = "SINGLE_CANDIDATE"
        edge["relationship_status"] = status
        edge["modis_degree"] = m_degree
        edge["viirs_degree"] = v_degree
        edge["component_id"] = component_id


def _unmatched_records(
    rows: Sequence[Mapping[str, Any]],
    matched_node_keys: set[tuple[str, int]],
    *,
    processing_run_id: int,
    matching_method: str,
    config_json: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        sensor = str(row["sensor"])
        observation_id = int(row["observation_id"])
        if (sensor, observation_id) in matched_node_keys:
            continue
        records.append({
            "processing_run_id": processing_run_id,
            "modis_observation_id": observation_id if sensor == "MODIS" else None,
            "viirs_observation_id": observation_id if sensor == "VIIRS" else None,
            "matching_method": matching_method,
            "time_difference_minutes": None,
            "centroid_distance_km": None,
            "intersection_area_km2": None,
            "iou": None,
            "smaller_footprint_overlap": None,
            "boundary_distance_km": None,
            "geometry_source": None,
            "relationship_status": "UNMATCHED",
            "modis_degree": 0 if sensor == "MODIS" else None,
            "viirs_degree": 0 if sensor == "VIIRS" else None,
            "component_id": None,
            "config_json": config_json,
        })
    return records


def _matching_run_config(
    *, method: str, method_version: str, bbox: BoundingBox | None,
    max_time_difference_minutes: float, **kwargs: Any,
) -> tuple[dict[str, Any], str]:
    config = {
        "module": "SANYAM_MODULE_6_MATCHING",
        "method": method,
        "method_version": method_version,
        "bbox": [bbox.west, bbox.south, bbox.east, bbox.north] if bbox else None,
        "max_time_difference_minutes": max_time_difference_minutes,
        "qc_filter": "latest_completed_qc.suitability_for_matching = true",
        "observation_identity": "satellite_observation_not_physical_fire_event",
        "evidence_category": "CANDIDATE_ASSOCIATION",
        "uncertainty_sources": [
            "geolocation", "temporal_difference", "matching_thresholds",
            "observation_opportunity", "footprint_representation",
        ],
        "physical_fire_identity_confirmed": False,
        **kwargs,
    }
    return config, json.dumps(config, sort_keys=True, separators=(",", ":"))


def _attach_candidate_explanations(
    records: list[dict[str, Any]], *, all_rows: Sequence[Mapping[str, Any]],
    query_rows: Sequence[Mapping[str, Any]], max_time_difference_minutes: float,
    max_distance_km: float | None = None,
) -> None:
    """Persist explainability, source provenance and one primary UTC day."""
    by_id = {int(row["observation_id"]): row for row in all_rows}
    query_ids = {int(row["observation_id"]) for row in query_rows}
    component_times: dict[str, list[datetime]] = defaultdict(list)
    for record in records:
        m_id, v_id = record.get("modis_observation_id"), record.get("viirs_observation_id")
        members = [by_id[int(value)] for value in (m_id, v_id) if value is not None and int(value) in by_id]
        component = record.get("component_id")
        if component and members:
            component_times[str(component)].extend(_utc(member["acquired_at"]) for member in members)
    for record in records:
        m_id, v_id = record.get("modis_observation_id"), record.get("viirs_observation_id")
        members = [by_id[int(value)] for value in (m_id, v_id) if value is not None and int(value) in by_id]
        warnings: list[dict[str, str]] = []
        if max_time_difference_minutes > 60:
            warnings.append({
                "code": "DIURNAL_DRIFT_WARNING",
                "message": f"The {max_time_difference_minutes:g}-minute window may span meaningful changes in fire intensity between overpasses",
            })
        if m_id is not None and v_id is not None and len(members) == 2:
            m_row = by_id[int(m_id)]
            v_row = by_id[int(v_id)]
            delta = float(record.get("time_difference_minutes") or 0.0)
            if record.get("relationship_status") == "REJECTED":
                reason = f"The pair was rejected because its time difference ({delta:.1f} minutes) or spatial criteria exceeded the configured limits."
            elif record.get("matching_method") == "FOOTPRINT_AWARE" and record.get("smaller_footprint_overlap") is not None:
                reason = (
                    f"The MODIS and VIIRS pixel footprints overlap by {float(record['smaller_footprint_overlap']):.2f} "
                    f"of the smaller footprint; Δt={delta:.1f} minutes."
                )
            else:
                distance = record.get("centroid_distance_km")
                reason = f"The pixel centres are {float(distance):.2f} km apart; Δt={delta:.1f} minutes."
            times = component_times.get(str(record.get("component_id")), [_utc(x["acquired_at"]) for x in members])
            earliest, latest = min(times), max(times)
            cross_midnight = earliest.date() != latest.date()
            primary_day = earliest.date().isoformat()
            if cross_midnight:
                reason += f" The association spans UTC midnight and is assigned once to {primary_day}."
            provenance = []
            for row in members:
                try:
                    raw = json.loads(str(row.get("raw_json") or "{}"))
                except (ValueError, TypeError):
                    raw = {}
                provenance.append({
                    "observation_id": row.get("observation_id"),
                    "sensor": row.get("sensor"),
                    "platform": _row_platform(row),
                    "granule_id": row.get("granule_id"),
                    "raw_asset_id": row.get("raw_asset_id"),
                    "sha256": row.get("raw_asset_sha256"),
                    "product_id": row.get("product_id"),
                    "collection": row.get("collection"),
                    "source_csv_path": raw.get("source_path") or raw.get("storage_path"),
                    "dataset_id": row.get("dataset_id"),
                    "geometry_source": row.get("footprint_source"),
                    "acquired_at_utc": _utc(row["acquired_at"]).isoformat(),
                })
        else:
            observation = members[0] if members else None
            cross_midnight = False
            primary_day = _utc(observation["acquired_at"]).date().isoformat() if observation else None
            sensor = observation.get("sensor") if observation else "unknown"
            reason = f"No opposite-sensor candidate satisfied the configured spatial and {max_time_difference_minutes:g}-minute temporal criteria for this {sensor} observation."
            provenance = []
            if observation:
                try:
                    raw = json.loads(str(observation.get("raw_json") or "{}"))
                except (ValueError, TypeError):
                    raw = {}
                provenance = [{
                    "observation_id": observation.get("observation_id"),
                    "sensor": observation.get("sensor"), "platform": _row_platform(observation),
                    "granule_id": observation.get("granule_id"), "raw_asset_id": observation.get("raw_asset_id"),
                    "sha256": observation.get("raw_asset_sha256"), "product_id": observation.get("product_id"),
                    "collection": observation.get("collection"),
                    "source_csv_path": raw.get("source_path") or raw.get("storage_path"),
                    "dataset_id": observation.get("dataset_id"),
                    "geometry_source": observation.get("footprint_source"),
                    "acquired_at_utc": _utc(observation["acquired_at"]).isoformat(),
                }]
        if warnings:
            reason += " Diurnal-drift caution: temporal separation exceeds 60 minutes."
        explanation = {
            "reason_code": "CANDIDATE_ASSOCIATION" if m_id is not None and v_id is not None else "UNMATCHED_OBSERVATION",
            "plain_language": reason,
            "provenance": provenance,
            "uncertainty_sources": ["geolocation", "granule timing precision", "temporal threshold", "spatial method", "observation opportunity"],
            "warnings": warnings,
        }
        record["is_cross_midnight"] = bool(cross_midnight)
        record["primary_calendar_day"] = primary_day
        record["explanation_json"] = json.dumps(explanation, sort_keys=True, separators=(",", ":"))


def run_baseline_matching(
    repository: DatabaseRepository,
    *,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bbox: str | Sequence[float] | BoundingBox | None = None,
    max_distance_km: float = DEFAULT_BASELINE_DISTANCE_KM,
    max_time_difference_minutes: float = DEFAULT_TIME_DIFFERENCE_MINUTES,
    qc_processing_run_id: int | None = None,
    method_version: str = METHOD_VERSION,
    platforms: Sequence[str] | None = None,
    data_source_mode: str = "BOTH",
    window_preset: int | str | None = None,
    temporal_window_minutes: float | None = None,
    grid_resolution_km: float | None = None,
) -> dict[str, Any]:
    """Run continuous-UTC centroid matching with padded query boundaries."""
    max_time_difference_minutes, temporal_source, temporal_preset = resolve_temporal_window(
        window_preset=window_preset, temporal_window_minutes=temporal_window_minutes,
        legacy_default=max_time_difference_minutes,
    )
    if max_distance_km <= 0:
        raise ValueError("distance must be > 0")
    if grid_resolution_km is not None and grid_resolution_km not in {1.0, 2.0, 5.0}:
        raise ValueError("grid_resolution_km must be 1.0, 2.0, 5.0, or None")
    selected_platforms = sorted(_normalise_platforms(platforms) or [])
    data_source_mode = data_source_mode.upper()
    box_value = BoundingBox.parse(bbox) if bbox is not None else None
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be supplied together")
    if start_date is not None and end_date is not None:
        start_at, end_at, start_day, end_day = _date_bounds(start_date, end_date)
    else:
        start_at = end_at = start_day = end_day = None

    rows = _filter_observations(
        repository, start_at=start_at, end_at=end_at, bbox=box_value,
        qc_processing_run_id=qc_processing_run_id, platforms=platforms,
        data_source_mode=data_source_mode, time_padding_minutes=max_time_difference_minutes,
    )
    query_rows = [row for row in rows if _within_window(row["acquired_at"], start_at, end_at)]
    modis_rows, viirs_rows = _partition_by_sensor(rows)
    diagnostics = _platform_diagnostics(platforms, start_at, end_at)
    config, config_json = _matching_run_config(
        method="BASELINE_HAVERSINE", method_version=method_version,
        bbox=box_value, max_time_difference_minutes=max_time_difference_minutes,
        max_distance_km=max_distance_km, qc_processing_run_id=qc_processing_run_id,
        start_date=start_day.isoformat() if start_day else None,
        end_date=end_day.isoformat() if end_day else None,
        earth_radius_km=EARTH_RADIUS_KM,
        selected_platforms=selected_platforms or ["ALL"], data_source_mode=data_source_mode,
        grid_resolution_km=grid_resolution_km,
        temporal_window_source=temporal_source, temporal_window_preset=temporal_preset,
        query_start_utc=start_at.isoformat() if start_at else None,
        query_end_utc=end_at.isoformat() if end_at else None,
        padded_fetch_start_utc=(start_at - timedelta(minutes=max_time_difference_minutes)).isoformat() if start_at else None,
        padded_fetch_end_utc=(end_at + timedelta(minutes=max_time_difference_minutes)).isoformat() if end_at else None,
        platform_diagnostics=diagnostics,
    )
    config["run_cache_key"] = _run_cache_key(config)
    config_json = json.dumps(config, sort_keys=True, separators=(",", ":"))
    run_id = repository.create_processing_run("MATCHING", method_version, config_json, run_cache_key=config["run_cache_key"])
    try:
        sorted_viirs = sorted(viirs_rows, key=lambda row: float(row["latitude"]))
        viirs_lats = [float(row["latitude"]) for row in sorted_viirs]
        latitude_window = math.degrees(max_distance_km / EARTH_RADIUS_KM) + 1e-9
        edges: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for modis in modis_rows:
            m_lat, m_lon = float(modis["latitude"]), float(modis["longitude"])
            lo = bisect.bisect_left(viirs_lats, m_lat - latitude_window)
            hi = bisect.bisect_right(viirs_lats, m_lat + latitude_window)
            for viirs in sorted_viirs[lo:hi]:
                distance = haversine_distance_km(
                    m_lon, m_lat, float(viirs["longitude"]), float(viirs["latitude"])
                )
                if distance > max_distance_km:
                    continue
                time_diff = _pair_time_difference_minutes(modis, viirs)
                record = {
                    "processing_run_id": run_id,
                    "modis_observation_id": int(modis["observation_id"]),
                    "viirs_observation_id": int(viirs["observation_id"]),
                    "matching_method": "BASELINE_HAVERSINE",
                    "time_difference_minutes": time_diff,
                    "centroid_distance_km": distance,
                    "intersection_area_km2": None,
                    "iou": None,
                    "smaller_footprint_overlap": None,
                    "boundary_distance_km": None,
                    "geometry_source": None,
                    "relationship_status": "REJECTED" if time_diff > max_time_difference_minutes else "SINGLE_CANDIDATE",
                    "modis_degree": None,
                    "viirs_degree": None,
                    "component_id": None,
                    "config_json": config_json,
                }
                if not _pair_touches_query_window(modis, viirs, start_at, end_at):
                    continue
                if time_diff <= max_time_difference_minutes:
                    edges.append(record)
                else:
                    rejected.append(record)
        _classify_edges(edges)
        matched_nodes: set[tuple[str, int]] = set()
        for edge in edges:
            matched_nodes.add(("MODIS", int(edge["modis_observation_id"])))
            matched_nodes.add(("VIIRS", int(edge["viirs_observation_id"])))
        records = edges + rejected + _unmatched_records(
            query_rows, matched_nodes, processing_run_id=run_id,
            matching_method="BASELINE_HAVERSINE", config_json=config_json,
        )
        _attach_candidate_explanations(
            records, all_rows=rows, query_rows=query_rows,
            max_time_difference_minutes=max_time_difference_minutes,
            max_distance_km=max_distance_km,
        )
        inserted = repository.insert_candidate_associations(records)
        repository.finish_processing_run(
            run_id, status="COMPLETED", input_count=len(rows), output_count=inserted
        )
    except Exception as exc:
        repository.finish_processing_run(
            run_id, status="FAILED", input_count=len(rows), output_count=0, error_message=str(exc)
        )
        raise
    statuses: dict[str, int] = defaultdict(int)
    for record in records:
        statuses[record["relationship_status"]] += 1
    return {
        "processing_run_id": run_id, "method": "BASELINE_HAVERSINE",
        "input_observations": len(rows), "candidate_edges": len(edges),
        "rejected_edges": len(rejected), "unmatched_observations": statuses["UNMATCHED"],
        "output_records": inserted, "relationship_status_counts": dict(statuses),
        "max_distance_km": max_distance_km,
        "max_time_difference_minutes": max_time_difference_minutes,
        "temporal_window_source": temporal_source,
        "temporal_window_preset": temporal_preset,
        "padded_fetch_observations": len(rows),
        "requested_window_observations": len(query_rows),
        "cross_midnight_candidate_count": sum(1 for item in records if item.get("is_cross_midnight")),
        "warnings": ([{"code": "DIURNAL_DRIFT_WARNING", "message": "Temporal window >60 minutes may span material fire-intensity change"}] if max_time_difference_minutes > 60 else []) + diagnostics,
        "run_cache_key": config["run_cache_key"],
        "status": "COMPLETED",
    }


def run_footprint_matching(
    repository: DatabaseRepository,
    *,
    bbox: str | Sequence[float] | BoundingBox,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    max_time_difference_minutes: float = DEFAULT_TIME_DIFFERENCE_MINUTES,
    max_boundary_distance_km: float = DEFAULT_BOUNDARY_DISTANCE_KM,
    fallback_centroid_distance_km: float = DEFAULT_FALLBACK_CENTROID_DISTANCE_KM,
    qc_processing_run_id: int | None = None,
    footprint_provider: FootprintProvider | None = None,
    method_version: str = METHOD_VERSION,
    platforms: Sequence[str] | None = None,
    data_source_mode: str = "BOTH",
    window_preset: int | str | None = None,
    temporal_window_minutes: float | None = None,
    grid_resolution_km: float | None = None,
) -> dict[str, Any]:
    """Run footprint-aware association in local LAEA using an injected provider.

    Valid scan/track values create axis-aligned LAEA rectangles. Missing or
    unusable geometry may use a separately labelled centroid fallback, never
    presented as a true pixel-corner footprint.
    """
    max_time_difference_minutes, temporal_source, temporal_preset = resolve_temporal_window(
        window_preset=window_preset, temporal_window_minutes=temporal_window_minutes,
        legacy_default=max_time_difference_minutes,
    )
    if max_boundary_distance_km < 0 or fallback_centroid_distance_km <= 0:
        raise ValueError("boundary limit must be >= 0 and centroid fallback must be > 0")
    if grid_resolution_km is not None and grid_resolution_km not in {1.0, 2.0, 5.0}:
        raise ValueError("grid_resolution_km must be 1.0, 2.0, 5.0, or None")
    selected_platforms = sorted(_normalise_platforms(platforms) or [])
    data_source_mode = data_source_mode.upper()
    box_value = BoundingBox.parse(bbox)
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be supplied together")
    if start_date is not None and end_date is not None:
        start_at, end_at, start_day, end_day = _date_bounds(start_date, end_date)
    else:
        start_at = end_at = start_day = end_day = None
    rows = _filter_observations(
        repository, start_at=start_at, end_at=end_at, bbox=box_value,
        qc_processing_run_id=qc_processing_run_id, platforms=platforms,
        data_source_mode=data_source_mode, time_padding_minutes=max_time_difference_minutes,
    )
    query_rows = [row for row in rows if _within_window(row["acquired_at"], start_at, end_at)]
    modis_rows, viirs_rows = _partition_by_sensor(rows)
    context = build_equal_area_context(box_value)
    provider_impl = footprint_provider or CompositeFootprintProvider()
    diagnostics = _platform_diagnostics(platforms, start_at, end_at)
    config, config_json = _matching_run_config(
        method="FOOTPRINT_AWARE", method_version=method_version, bbox=box_value,
        max_time_difference_minutes=max_time_difference_minutes,
        max_boundary_distance_km=max_boundary_distance_km,
        fallback_centroid_distance_km=fallback_centroid_distance_km,
        qc_processing_run_id=qc_processing_run_id,
        start_date=start_day.isoformat() if start_day else None,
        end_date=end_day.isoformat() if end_day else None,
        selected_platforms=selected_platforms or ["ALL"], data_source_mode=data_source_mode,
        grid_resolution_km=grid_resolution_km,
        temporal_window_source=temporal_source, temporal_window_preset=temporal_preset,
        query_start_utc=start_at.isoformat() if start_at else None,
        query_end_utc=end_at.isoformat() if end_at else None,
        padded_fetch_start_utc=(start_at - timedelta(minutes=max_time_difference_minutes)).isoformat() if start_at else None,
        padded_fetch_end_utc=(end_at + timedelta(minutes=max_time_difference_minutes)).isoformat() if end_at else None,
        platform_diagnostics=diagnostics,
        geometry_source=provider_impl.geometry_source,
        projection=context.crs.to_wkt(),
        geometry_note=(
            "FIRMS scan/track dimensions reconstructed as axis-aligned local equal-area rectangles; "
            "approximate proxy, not Level-2 pixel corners."
            if provider_impl.geometry_source == FIRMS_GEOMETRY_SOURCE else
            "Provider-supplied geometry. The provider is responsible for retaining and documenting "
            "its source geolocation and geometry-construction method."
        ),
    )
    config["run_cache_key"] = _run_cache_key(config)
    config_json = json.dumps(config, sort_keys=True, separators=(",", ":"))
    run_id = repository.create_processing_run("MATCHING", method_version, config_json, run_cache_key=config["run_cache_key"])
    try:
        modis_geoms: dict[int, Polygon] = {}
        viirs_geoms: dict[int, Polygon] = {}
        for row in modis_rows:
            geometry = provider_impl.build_polygon(row, context)
            if geometry is not None:
                modis_geoms[int(row["observation_id"])] = geometry
        for row in viirs_rows:
            geometry = provider_impl.build_polygon(row, context)
            if geometry is not None:
                viirs_geoms[int(row["observation_id"])] = geometry

        # STRtree limits exact polygon comparisons to spatially nearby polygons.
        viirs_geom_rows = [row for row in viirs_rows if int(row["observation_id"]) in viirs_geoms]
        viirs_polygons = [viirs_geoms[int(row["observation_id"])] for row in viirs_geom_rows]
        tree = STRtree(viirs_polygons) if viirs_polygons else None
        edges: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        seen_pairs: set[tuple[int, int]] = set()
        boundary_m = max_boundary_distance_km * 1000.0

        for modis in modis_rows:
            m_id = int(modis["observation_id"])
            m_poly = modis_geoms.get(m_id)
            if m_poly is None or tree is None:
                continue
            candidate_indices = tree.query(m_poly.buffer(boundary_m))
            for raw_idx in candidate_indices:
                idx = int(raw_idx)
                viirs = viirs_geom_rows[idx]
                v_id = int(viirs["observation_id"])
                pair = (m_id, v_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                if not _pair_touches_query_window(modis, viirs, start_at, end_at):
                    continue
                v_poly = viirs_geoms[v_id]
                time_diff = _pair_time_difference_minutes(modis, viirs)
                centroid_distance = m_poly.centroid.distance(v_poly.centroid) / 1000.0
                metrics = footprint_metrics(m_poly, v_poly)
                qualifies_geometry = (
                    metrics["intersection_area_km2"] > 0.0
                    or metrics["boundary_distance_km"] <= max_boundary_distance_km
                )
                record = {
                    "processing_run_id": run_id,
                    "modis_observation_id": m_id,
                    "viirs_observation_id": v_id,
                    "matching_method": "FOOTPRINT_AWARE",
                    "time_difference_minutes": time_diff,
                    "centroid_distance_km": centroid_distance,
                    **metrics,
                    "geometry_source": provider_impl.geometry_source,
                    "relationship_status": "SINGLE_CANDIDATE",
                    "modis_degree": None,
                    "viirs_degree": None,
                    "component_id": None,
                    "config_json": config_json,
                }
                if time_diff <= max_time_difference_minutes and qualifies_geometry:
                    edges.append(record)
                else:
                    record["relationship_status"] = "REJECTED"
                    rejected.append(record)

        # Explicit, labelled centroid fallback only for pairs where at least one
        # side lacks a usable FIRMS rectangle. Point fallback remains distinct.
        modis_without = [row for row in modis_rows if int(row["observation_id"]) not in modis_geoms]
        viirs_without = [row for row in viirs_rows if int(row["observation_id"]) not in viirs_geoms]
        if modis_without and viirs_rows or viirs_without and modis_rows:
            sorted_viirs = sorted(viirs_rows, key=lambda row: float(row["latitude"]))
            viirs_lats = [float(row["latitude"]) for row in sorted_viirs]
            lat_window = math.degrees(fallback_centroid_distance_km / EARTH_RADIUS_KM) + 1e-9
            for modis in modis_without:
                m_id = int(modis["observation_id"])
                lat = float(modis["latitude"])
                lo, hi = bisect.bisect_left(viirs_lats, lat - lat_window), bisect.bisect_right(viirs_lats, lat + lat_window)
                for viirs in sorted_viirs[lo:hi]:
                    v_id = int(viirs["observation_id"])
                    # Any pair with at least one unavailable footprint may use
                    # the explicitly labelled centroid fallback. Do not skip a
                    # VIIRS polygon here: a MODIS point fallback can still be
                    # compared with a real/provider-supplied VIIRS footprint.
                    if (m_id, v_id) in seen_pairs:
                        continue
                    distance = haversine_distance_km(
                        float(modis["longitude"]), lat,
                        float(viirs["longitude"]), float(viirs["latitude"]),
                    )
                    if distance > fallback_centroid_distance_km:
                        continue
                    time_diff = _pair_time_difference_minutes(modis, viirs)
                    fallback_record = {
                        "processing_run_id": run_id,
                        "modis_observation_id": m_id,
                        "viirs_observation_id": v_id,
                        "matching_method": "FOOTPRINT_AWARE",
                        "time_difference_minutes": time_diff,
                        "centroid_distance_km": distance,
                        "intersection_area_km2": None,
                        "iou": None,
                        "smaller_footprint_overlap": None,
                        "boundary_distance_km": None,
                        "geometry_source": POINT_FALLBACK_GEOMETRY_SOURCE,
                        "relationship_status": "SINGLE_CANDIDATE" if time_diff <= max_time_difference_minutes else "REJECTED",
                        "modis_degree": None,
                        "viirs_degree": None,
                        "component_id": None,
                        "config_json": config_json,
                    }
                    seen_pairs.add((m_id, v_id))
                    if not _pair_touches_query_window(modis, viirs, start_at, end_at):
                        continue
                    if time_diff <= max_time_difference_minutes:
                        edges.append(fallback_record)
                    else:
                        rejected.append(fallback_record)
            # VIIRS-only missing-footprint records paired with MODIS polygons.
            # Iterate MODIS centres with a latitude window; avoid duplicating
            # pairs already tested from the MODIS-missing side.
            if viirs_without:
                sorted_modis = sorted(modis_rows, key=lambda row: float(row["latitude"]))
                modis_lats = [float(row["latitude"]) for row in sorted_modis]
                for viirs in viirs_without:
                    v_id = int(viirs["observation_id"])
                    lat = float(viirs["latitude"])
                    lo, hi = bisect.bisect_left(modis_lats, lat - lat_window), bisect.bisect_right(modis_lats, lat + lat_window)
                    for modis in sorted_modis[lo:hi]:
                        m_id = int(modis["observation_id"])
                        if (m_id, v_id) in seen_pairs:
                            continue
                        distance = haversine_distance_km(
                            float(modis["longitude"]), float(modis["latitude"]),
                            float(viirs["longitude"]), lat,
                        )
                        if distance > fallback_centroid_distance_km:
                            continue
                        time_diff = _pair_time_difference_minutes(modis, viirs)
                        fallback_record = {
                            "processing_run_id": run_id,
                            "modis_observation_id": m_id,
                            "viirs_observation_id": v_id,
                            "matching_method": "FOOTPRINT_AWARE",
                            "time_difference_minutes": time_diff,
                            "centroid_distance_km": distance,
                            "intersection_area_km2": None,
                            "iou": None,
                            "smaller_footprint_overlap": None,
                            "boundary_distance_km": None,
                            "geometry_source": POINT_FALLBACK_GEOMETRY_SOURCE,
                            "relationship_status": "SINGLE_CANDIDATE" if time_diff <= max_time_difference_minutes else "REJECTED",
                            "modis_degree": None,
                            "viirs_degree": None,
                            "component_id": None,
                            "config_json": config_json,
                        }
                        seen_pairs.add((m_id, v_id))
                        if not _pair_touches_query_window(modis, viirs, start_at, end_at):
                            continue
                        if time_diff <= max_time_difference_minutes:
                            edges.append(fallback_record)
                        else:
                            rejected.append(fallback_record)

        _classify_edges(edges)
        matched_nodes: set[tuple[str, int]] = set()
        for edge in edges:
            matched_nodes.add(("MODIS", int(edge["modis_observation_id"])))
            matched_nodes.add(("VIIRS", int(edge["viirs_observation_id"])))
        records = edges + rejected + _unmatched_records(
            query_rows, matched_nodes, processing_run_id=run_id,
            matching_method="FOOTPRINT_AWARE", config_json=config_json,
        )
        _attach_candidate_explanations(
            records, all_rows=rows, query_rows=query_rows,
            max_time_difference_minutes=max_time_difference_minutes,
        )
        # Geometry provenance is pair-specific because one side can be a true
        # Level-2 corner polygon while the other uses a FIRMS approximation.
        source_by_id = {int(row["observation_id"]): provider_impl.source_for(row) for row in rows} if isinstance(provider_impl, CompositeFootprintProvider) else {}
        for record in records:
            m_id, v_id = record.get("modis_observation_id"), record.get("viirs_observation_id")
            sources = [source_by_id.get(int(value)) for value in (m_id, v_id) if value is not None and int(value) in source_by_id]
            if len(sources) == 2 and sources[0] == sources[1]:
                record["geometry_source"] = sources[0]
            elif sources and LEVEL2_GEOMETRY_SOURCE in sources:
                record["geometry_source"] = "MIXED_LEVEL2_AND_FIRMS_GEOMETRY" if len(set(sources)) > 1 else LEVEL2_GEOMETRY_SOURCE
        inserted = repository.insert_candidate_associations(records)
        repository.finish_processing_run(
            run_id, status="COMPLETED", input_count=len(rows), output_count=inserted
        )
    except Exception as exc:
        repository.finish_processing_run(
            run_id, status="FAILED", input_count=len(rows), output_count=0, error_message=str(exc)
        )
        raise
    status_counts: dict[str, int] = defaultdict(int)
    for record in records:
        status_counts[record["relationship_status"]] += 1
    return {
        "processing_run_id": run_id, "method": "FOOTPRINT_AWARE",
        "input_observations": len(rows), "candidate_edges": len(edges),
        "rejected_edges": len(rejected), "unmatched_observations": status_counts["UNMATCHED"],
        "output_records": inserted, "relationship_status_counts": dict(status_counts),
        "geometry_source": provider_impl.geometry_source,
        "fallback_geometry_source": POINT_FALLBACK_GEOMETRY_SOURCE,
        "projection": context.crs.to_string(), "temporal_window_source": temporal_source,
        "temporal_window_preset": temporal_preset,
        "padded_fetch_observations": len(rows), "requested_window_observations": len(query_rows),
        "cross_midnight_candidate_count": sum(1 for item in records if item.get("is_cross_midnight")),
        "warnings": ([{"code": "DIURNAL_DRIFT_WARNING", "message": "Temporal window >60 minutes may span material fire-intensity change"}] if max_time_difference_minutes > 60 else []) + diagnostics,
        "run_cache_key": config["run_cache_key"], "status": "COMPLETED",
    }


class FRPObservationOperator(Protocol):
    """Hook for a validated FRP_OOp implementation.

    Implementations may expose ``adjust_with_context(observation, inputs)`` to
    receive pixel area, day/night, background temperatures and beta_ret. The
    legacy ``adjust(observation, pixel_area_km2)`` hook remains supported.
    """

    name: str
    version: str

    def adjust(self, observation: Mapping[str, Any], pixel_area_km2: float | None) -> float | None:
        """Return an adjusted FRP in MW, or None when unsupported/undetermined."""


def _collection_key(observation: Mapping[str, Any]) -> str | None:
    value = observation.get("collection")
    if value is None:
        # A conservative fallback for already-ingested older rows. Do not infer
        # from platform alone; only use source version strings with known prefix.
        version = str(observation.get("source_version") or "")
        if version.startswith("6.1"):
            return "6.1"
        if version.startswith("2"):
            return "2"
        return None
    raw = str(value).strip()
    if raw.startswith("6.1"):
        return "6.1"
    if raw.startswith("2"):
        return "2"
    return raw or None


def estimate_frp_detection_limit_mw(
    observation: Mapping[str, Any], pixel_area_km2: float | None,
) -> float | None:
    """Initial diagnostic limit scaled by the FIRMS scan*track/reference area ratio.

    Reference values from SKH §7.5.5: MODIS C6.1 5.7 MW day / 3.7 MW night;
    VIIRS C2 3.7 MW day / 0.7 MW night, for the specified smallest central-swath
    pixels. A simple linear area-ratio scaling is only a transparent diagnostic
    approximation. It is not the full published pixel/grid observation operator.
    """
    sensor = str(observation.get("sensor") or "").upper()
    collection = _collection_key(observation)
    daynight = str(observation.get("daynight") or "").upper()
    if daynight not in {"D", "N"} or pixel_area_km2 is None or pixel_area_km2 <= 0:
        return None
    if sensor == "MODIS" and collection == "6.1":
        base_mw = 5.7 if daynight == "D" else 3.7
        reference_area = 1.0  # nominal ~1 km x 1 km central-swath reference pixel
    elif sensor == "VIIRS" and collection == "2":
        base_mw = 3.7 if daynight == "D" else 0.7
        reference_area = 0.375 * 0.375
    else:
        return None
    return base_mw * (pixel_area_km2 / reference_area)


def _observation_pixel_area_km2(row: Mapping[str, Any]) -> float | None:
    if row.get("footprint_wkt"):
        try:
            geometry = load_wkt(str(row["footprint_wkt"]))
            context = build_equal_area_context(BoundingBox.parse((
                max(-179.9, float(row["longitude"]) - 0.05),
                max(-89.9, float(row["latitude"]) - 0.05),
                min(179.9, float(row["longitude"]) + 0.05),
                min(89.9, float(row["latitude"]) + 0.05),
            )))
            projected = transform_geometry(context.forward.transform, geometry)
            area = projected.area / 1_000_000.0
            if math.isfinite(area) and area > 0:
                return area
        except Exception:
            pass
    dimensions = _positive_scan_track(row)
    return dimensions[0] * dimensions[1] if dimensions else None


def analyze_frp_measurements(
    repository: DatabaseRepository,
    *,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bbox: str | Sequence[float] | BoundingBox | None = None,
    qc_processing_run_id: int | None = None,
    operator: FRPObservationOperator | None = None,
    method_version: str = FRP_DIAGNOSTIC_VERSION,
) -> dict[str, Any]:
    """Store source FRP unchanged plus detection-limit diagnostics and optional operator output."""
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be supplied together")
    if start_date is not None and end_date is not None:
        start_at, end_at, start_day, end_day = _date_bounds(start_date, end_date)
    else:
        start_at = end_at = start_day = end_day = None
    box_value = BoundingBox.parse(bbox) if bbox is not None else None
    rows = repository.fetch_scientific_observations(
        sensors=("MODIS", "VIIRS"), start_at=start_at, end_at=end_at,
        west=box_value.west if box_value else None,
        south=box_value.south if box_value else None,
        east=box_value.east if box_value else None,
        north=box_value.north if box_value else None,
        qc_processing_run_id=qc_processing_run_id,
        require_matching_suitability=False,
        require_frp_suitability=False,
    )
    config = {
        "module": "SANYAM_FRP_MEASUREMENT_DIAGNOSTIC",
        "method_version": method_version,
        "operator": getattr(operator, "name", None),
        "operator_version": getattr(operator, "version", None),
        "raw_frp_policy": "copy observations.frp_mw without adjustment",
        "detection_limit_policy": "reference values linearly scaled by pixel_area/reference_pixel_area; diagnostic only",
        "qc_processing_run_id": qc_processing_run_id,
        "bbox": [box_value.west, box_value.south, box_value.east, box_value.north] if box_value else None,
        "start_date": start_day.isoformat() if start_day else None,
        "end_date": end_day.isoformat() if end_day else None,
    }
    config_json = json.dumps(config, sort_keys=True, separators=(",", ":"))
    run_id = repository.create_processing_run("MEASUREMENT", method_version, config_json)
    try:
        payload: list[dict[str, Any]] = []
        for row in rows:
            pixel_area = _observation_pixel_area_km2(row)
            detection_limit = estimate_frp_detection_limit_mw(row, pixel_area)
            try:
                native_attributes = json.loads(str(row.get("native_attributes_json") or "{}"))
            except (TypeError, json.JSONDecodeError):
                native_attributes = {}
            try:
                solar_zenith = float(native_attributes["FP_SolZenAng"]) if native_attributes.get("FP_SolZenAng") is not None else None
                if solar_zenith is not None and not math.isfinite(solar_zenith):
                    solar_zenith = None
            except (TypeError, ValueError):
                solar_zenith = None
            operator_inputs = {
                "pixel_area_km2": pixel_area,
                "daynight": row.get("daynight"),
                "solar_flag": row.get("daynight"),
                "solar_zenith_degrees": solar_zenith,
                "mean_t4_k": row.get("mean_t4_k"),
                "mean_t5_k": row.get("mean_t5_k"),
                # beta_ret is deliberately null until a Level-2 opportunity
                # result for the same spatial unit and interval is joined. A
                # bbox-wide fraction must not be reused as a grid-cell value.
                "beta_ret": row.get("beta_ret"),
                "beta_ret_status": "UNAVAILABLE_NO_SPATIOTEMPORAL_OPPORTUNITY_JOIN" if row.get("beta_ret") is None else "PROVIDED",
                "data_source": "LAADS_LEVEL2" if row.get("raw_asset_id") is not None else "FIRMS_CSV",
            }
            adjusted = None
            adjustment_method = None
            # A model/operator adjustment must not use an observation that
            # failed the dedicated FRP suitability rules. We still retain its
            # native FRP (if present) and the reason is available in QC.
            if (
                operator is not None
                and bool(row.get("suitability_for_frp"))
                and row.get("frp_mw") is not None
            ):
                if hasattr(operator, "adjust_with_context"):
                    adjusted = operator.adjust_with_context(row, operator_inputs)
                else:
                    adjusted = operator.adjust(row, pixel_area)
                if adjusted is not None:
                    adjusted = float(adjusted)
                    if not math.isfinite(adjusted) or adjusted < 0:
                        raise ValueError("FRP observation operator must return a finite nonnegative MW value or None")
                    adjustment_method = str(getattr(operator, "name", operator.__class__.__name__))
            raw_value = row.get("frp_mw")
            raw_frp = float(raw_value) if raw_value is not None else None
            payload.append({
                "processing_run_id": run_id,
                "observation_id": int(row["observation_id"]),
                "frp_raw_mw": raw_frp,
                "frp_adjusted_mw": adjusted,
                "adjustment_method": adjustment_method,
                "method_version": method_version,
                "detection_limit_mw": detection_limit,
                "pixel_area_km2": pixel_area,
                "operator_inputs_json": json.dumps(operator_inputs, sort_keys=True, separators=(",", ":")),
                "diagnostic_json": json.dumps({"source_frp_preserved": raw_frp if (raw_frp := (float(row["frp_mw"]) if row.get("frp_mw") is not None else None)) is not None else None,
                                               "operator_applied": adjusted is not None,
                                               "geometry_source": row.get("footprint_source")}, sort_keys=True),
                "evidence_category": "MODEL_ADJUSTED" if adjusted is not None else "DIRECT_SOURCE_OBSERVATION",
            })
        inserted = repository.insert_measurement_results(payload)
        repository.finish_processing_run(run_id, status="COMPLETED", input_count=len(rows), output_count=inserted)
    except Exception as exc:
        repository.finish_processing_run(run_id, status="FAILED", input_count=len(rows), output_count=0, error_message=str(exc))
        raise
    return {
        "processing_run_id": run_id, "module_name": "MEASUREMENT",
        "input_count": len(rows), "output_count": inserted,
        "operator_applied": operator is not None,
        "adjusted_count": sum(1 for item in payload if item["frp_adjusted_mw"] is not None),
        "detection_limit_count": sum(1 for item in payload if item["detection_limit_mw"] is not None),
        "status": "COMPLETED",
    }


def _period_windows(start_day: date, end_day: date, period_days: int) -> list[tuple[datetime, datetime]]:
    if period_days < 1:
        raise ValueError("period_days must be >= 1")
    result = []
    current = start_day
    while current <= end_day:
        next_day = min(current + timedelta(days=period_days), end_day + timedelta(days=1))
        result.append((
            datetime.combine(current, time.min, tzinfo=timezone.utc),
            datetime.combine(next_day, time.min, tzinfo=timezone.utc),
        ))
        current = next_day
    return result


def _allocate_grid(
    pixel_polygon: Polygon,
    grid_tree: STRtree,
    grid_polygons: Sequence[Any],
) -> list[tuple[int, float]]:
    area = pixel_polygon.area
    if area <= 0:
        return []
    allocations = []
    for raw_index in grid_tree.query(pixel_polygon):
        index = int(raw_index)
        intersection_area = pixel_polygon.intersection(grid_polygons[index]).area
        if intersection_area > 0:
            allocations.append((index, min(1.0, intersection_area / area)))
    return allocations


def _components_for_associations(
    associations: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], str]:
    edges = [row for row in associations if row.get("relationship_status") not in {"UNMATCHED", "REJECTED"}
             and row.get("modis_observation_id") is not None and row.get("viirs_observation_id") is not None]
    _, component_ids, _ = _candidate_graph_components(edges)
    return component_ids


def build_common_grid_summary(
    repository: DatabaseRepository,
    *,
    bbox: str | Sequence[float] | BoundingBox,
    start_date: date | str,
    end_date: date | str,
    grid_resolution_km: float = DEFAULT_GRID_RESOLUTION_KM,
    period_days: int = 1,
    matching_run_id: int | None = None,
    qc_processing_run_id: int | None = None,
    footprint_provider: FootprintProvider | None = None,
    method_version: str = METHOD_VERSION,
    platforms: Sequence[str] | None = None,
    data_source_mode: str = "BOTH",
) -> dict[str, Any]:
    """Build a sparse, equal-area, footprint-weighted activity grid.

    Only grid-cell/period combinations intersected by at least one eligible
    detection are persisted. Unmaterialized cells are implicitly UNKNOWN for
    FIRMS-only evidence; they are not interpreted as valid non-detections.
    This sparse representation avoids allocating the full grid x period
    Cartesian product, which can contain millions of empty rows for regional
    daily analyses.

    Counts are fractional pixel-equivalent allocations. FRP is allocated by
    footprint intersection but remains a sensor-specific reported measurement;
    it is not a new native cell measurement and is not FRE. FIRMS-only
    valid_area_fraction is always NULL.
    """
    if grid_resolution_km not in {1.0, 2.0, 5.0}:
        raise ValueError("grid_resolution_km must be one of 1.0, 2.0, or 5.0 km")
    if period_days < 1:
        raise ValueError("period_days must be >= 1")
    box_value = BoundingBox.parse(bbox)
    start_at, end_at, start_day, end_day = _date_bounds(start_date, end_date)
    context = build_equal_area_context(box_value)
    provider_impl = footprint_provider or CompositeFootprintProvider()
    selected_platforms = sorted(_normalise_platforms(platforms) or [])
    data_source_mode = data_source_mode.upper()
    if data_source_mode not in {"FIRMS", "LAADS_LEVEL2", "BOTH"}:
        raise ValueError("data_source_mode must be FIRMS, LAADS_LEVEL2, or BOTH")
    matching_run = None
    matching_config: dict[str, Any] = {}
    if matching_run_id is not None:
        matching_run = repository.get_processing_run(matching_run_id)
        if matching_run:
            try:
                matching_config = json.loads(matching_run.get("configuration_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                matching_config = {}
        if not selected_platforms:
            inherited = matching_config.get("selected_platforms") or []
            selected_platforms = sorted(_normalise_platforms(inherited) or []) if inherited != ["ALL"] else []
        if data_source_mode == "BOTH" and matching_config.get("data_source_mode"):
            data_source_mode = str(matching_config["data_source_mode"]).upper()

    config = {
        "module": "SANYAM_COMMON_GRID_ACTIVITY",
        "method_version": method_version,
        "bbox": [box_value.west, box_value.south, box_value.east, box_value.north],
        "start_date": start_day.isoformat(), "end_date": end_day.isoformat(),
        "grid_resolution_km": grid_resolution_km, "period_days": period_days,
        "projection": context.crs.to_wkt(),
        "geometry_source": provider_impl.geometry_source,
        "spatial_allocation": "Area(pixel footprint intersect grid cell) / Area(pixel footprint)",
        "count_semantics": "footprint-weighted pixel-equivalent count",
        "frp_semantics": "footprint-allocated original FRP; not independently measured per cell; not FRE",
        "valid_area_fraction": None,
        "opportunity_evidence": "FIRMS_DETECTION_ONLY",
        "matching_run_id": matching_run_id,
        "qc_processing_run_id": qc_processing_run_id,
        "selected_platforms": selected_platforms or ["ALL"],
        "data_source_mode": data_source_mode,
        "temporal_window_minutes": matching_config.get("max_time_difference_minutes"),
        "spatial_method": matching_config.get("method", "GRID_ONLY"),
        "no_detection_policy": "UNKNOWN unless geolocated Level-2 swath mask evidence is provided; FIRMS point-only records cannot establish valid non-detection",
        "storage_strategy": "SPARSE_DETECTED_CELL_PERIODS; omitted cells are implicitly UNKNOWN",
        "uncertainty_sources": [
            "approximate FIRMS scan/track footprint" if provider_impl.geometry_source == FIRMS_GEOMETRY_SOURCE else "provider footprint/geolocation",
            "grid resolution", "FIRMS-only opportunity", "spatial allocation",
        ],
    }
    config["run_cache_key"] = _run_cache_key({
        "selected_platforms": selected_platforms or ["ALL"],
        "max_time_difference_minutes": matching_config.get("max_time_difference_minutes"),
        "method": matching_config.get("method", "GRID_ONLY"),
        "grid_resolution_km": grid_resolution_km,
        "data_source_mode": data_source_mode,
    })
    config_json = json.dumps(config, sort_keys=True, separators=(",", ":"))
    if matching_run_id is not None:
        matching_run = repository.get_processing_run(matching_run_id)
        if matching_run is None:
            raise KeyError(f"Matching processing run {matching_run_id} does not exist in this DATABASE_URL")
        if matching_run.get("module_name") != "MATCHING":
            raise ValueError(
                f"--matching-run-id {matching_run_id} refers to module "
                f"{matching_run.get('module_name')!r}, not 'MATCHING'"
            )
        if matching_run.get("status") != "COMPLETED":
            raise ValueError(
                f"Matching run {matching_run_id} is not COMPLETED "
                f"(status={matching_run.get('status')!r})"
            )

    run_id = repository.create_processing_run("AGGREGATION", method_version, config_json, run_cache_key=config["run_cache_key"])

    try:
        # The query pad allows a footprint whose centre lies just outside the
        # study bbox to contribute to an intersecting edge cell.
        pad_deg = 0.5
        query_west = max(-180.0, box_value.west - pad_deg)
        query_east = min(180.0, box_value.east + pad_deg)
        query_south = max(-90.0, box_value.south - pad_deg)
        query_north = min(90.0, box_value.north + pad_deg)
        observations = repository.fetch_scientific_observations(
            sensors=("MODIS", "VIIRS"), start_at=start_at, end_at=end_at,
            west=query_west, east=query_east, south=query_south, north=query_north,
            qc_processing_run_id=qc_processing_run_id,
            require_matching_suitability=True,
            data_source_mode=data_source_mode,
        )
        if selected_platforms:
            allowed_platforms = set(selected_platforms)
            observations = [row for row in observations if _row_platform(row) in allowed_platforms]

        size_m = grid_resolution_km * 1000.0
        minx, miny, maxx, maxy = context.study_polygon.bounds
        origin_x = math.floor(minx / size_m) * size_m
        origin_y = math.floor(miny / size_m) * size_m
        cols = int(math.ceil((maxx - origin_x) / size_m))
        rows_count = int(math.ceil((maxy - origin_y) / size_m))
        grid_bbox_slots = rows_count * cols  # Upper bound; edge slots may lie outside the study polygon.

        periods = _period_windows(start_day, end_day, period_days)
        period_starts = [window[0] for window in periods]

        # Only cache the cell polygons actually touched by an observation.
        cell_geometries: dict[tuple[int, int], Any] = {}
        accumulators: dict[tuple[int, int, int], dict[str, Any]] = {}

        def get_cell(row_index: int, col_index: int):
            key = (row_index, col_index)
            if key in cell_geometries:
                return cell_geometries[key]
            if not (0 <= row_index < rows_count and 0 <= col_index < cols):
                return None
            candidate = box(
                origin_x + col_index * size_m,
                origin_y + row_index * size_m,
                origin_x + (col_index + 1) * size_m,
                origin_y + (row_index + 1) * size_m,
            ).intersection(context.study_polygon)
            if candidate.is_empty or candidate.area <= 0:
                return None
            if not candidate.is_valid:
                candidate = make_valid(candidate)
            if candidate.is_empty or candidate.area <= 0:
                return None
            cell_geometries[key] = candidate
            return candidate

        def accumulator(period_index: int, row_index: int, col_index: int):
            key = (period_index, row_index, col_index)
            if key not in accumulators:
                accumulators[key] = {
                    "modis_count": 0.0, "viirs_count": 0.0,
                    "modis_frp_sum_mw": 0.0, "viirs_frp_sum_mw": 0.0,
                    "has_modis_frp": False, "has_viirs_frp": False,
                    "group_ids": set(), "cross_midnight_group_ids": set(),
                    "primary_calendar_days": set(), "centroid_fallback_count": 0,
                }
            return accumulators[key]

        def cell_range(bounds: tuple[float, float, float, float]):
            bx0, by0, bx1, by1 = bounds
            c0 = max(0, int(math.floor((bx0 - origin_x) / size_m)))
            c1 = min(cols - 1, int(math.floor((bx1 - origin_x) / size_m)))
            r0 = max(0, int(math.floor((by0 - origin_y) / size_m)))
            r1 = min(rows_count - 1, int(math.floor((by1 - origin_y) / size_m)))
            if c1 < c0 or r1 < r0:
                return range(0), range(0)
            return range(r0, r1 + 1), range(c0, c1 + 1)

        component_map: dict[tuple[str, int], str] = {}
        component_calendar: dict[str, dict[str, Any]] = {}
        if matching_run_id is not None:
            association_rows = repository.fetch_candidate_associations(matching_run_id)
            component_map = _components_for_associations(association_rows)
            for association in association_rows:
                component_id = association.get("component_id")
                if not component_id:
                    continue
                meta = component_calendar.setdefault(str(component_id), {
                    "primary_calendar_day": association.get("primary_calendar_day"),
                    "is_cross_midnight": False,
                })
                if association.get("is_cross_midnight"):
                    meta["is_cross_midnight"] = True
                primary_day = association.get("primary_calendar_day")
                if primary_day and (not meta.get("primary_calendar_day") or primary_day < meta["primary_calendar_day"]):
                    meta["primary_calendar_day"] = primary_day

        for observation in observations:
            acquired = _utc(observation["acquired_at"])
            period_index = bisect.bisect_right(period_starts, acquired) - 1
            if period_index < 0 or acquired >= periods[period_index][1]:
                continue

            sensor = str(observation["sensor"])
            observation_id = int(observation["observation_id"])
            component_id = component_map.get((sensor, observation_id))
            count_key = "modis_count" if sensor == "MODIS" else "viirs_count"
            frp_key = "modis_frp_sum_mw" if sensor == "MODIS" else "viirs_frp_sum_mw"
            has_key = "has_modis_frp" if sensor == "MODIS" else "has_viirs_frp"

            polygon = provider_impl.build_polygon(observation, context)
            if polygon is not None and not polygon.is_empty and polygon.area > 0:
                area = float(polygon.area)  # Projected equal-area square metres.
                candidate_rows, candidate_cols = cell_range(polygon.bounds)
                for row_index in candidate_rows:
                    for col_index in candidate_cols:
                        cell = get_cell(row_index, col_index)
                        if cell is None:
                            continue
                        intersection_area = float(polygon.intersection(cell).area)
                        if intersection_area <= 0:
                            continue
                        weight = min(1.0, intersection_area / area)
                        acc = accumulator(period_index, row_index, col_index)
                        acc[count_key] += weight
                        if bool(observation.get("suitability_for_frp")) and observation.get("frp_mw") is not None:
                            acc[frp_key] += float(observation["frp_mw"]) * weight
                            acc[has_key] = True
                        if component_id:
                            meta = component_calendar.get(component_id, {})
                            primary_day = meta.get("primary_calendar_day") or acquired.date().isoformat()
                            primary_in_period = (
                                periods[period_index][0]
                                <= datetime.combine(date.fromisoformat(primary_day), time.min, tzinfo=timezone.utc)
                                < periods[period_index][1]
                            )
                            if primary_in_period:
                                acc["group_ids"].add(component_id)
                                acc["primary_calendar_days"].add(primary_day)
                                if meta.get("is_cross_midnight"):
                                    acc["cross_midnight_group_ids"].add(component_id)
                continue

            # Explicit point-centroid fallback when scan/track footprint
            # dimensions are unavailable; it is never presented as an area.
            x, y = context.forward.transform(
                float(observation["longitude"]), float(observation["latitude"])
            )
            point = Point(x, y)
            if not context.study_polygon.covers(point):
                continue
            col_index = int(math.floor((x - origin_x) / size_m))
            row_index = int(math.floor((y - origin_y) / size_m))
            # Include points exactly on the maximum grid extent in the last cell.
            col_index = min(cols - 1, col_index)
            row_index = min(rows_count - 1, row_index)
            cell = get_cell(row_index, col_index)
            if cell is None or not cell.covers(point):
                continue
            acc = accumulator(period_index, row_index, col_index)
            acc["centroid_fallback_count"] += 1
            acc[count_key] += 1.0
            if bool(observation.get("suitability_for_frp")) and observation.get("frp_mw") is not None:
                acc[frp_key] += float(observation["frp_mw"])
                acc[has_key] = True
            if component_id:
                meta = component_calendar.get(component_id, {})
                primary_day = meta.get("primary_calendar_day") or acquired.date().isoformat()
                primary_in_period = (
                    periods[period_index][0]
                    <= datetime.combine(date.fromisoformat(primary_day), time.min, tzinfo=timezone.utc)
                    < periods[period_index][1]
                )
                if primary_in_period:
                    acc["group_ids"].add(component_id)
                    acc["primary_calendar_days"].add(primary_day)
                    if meta.get("is_cross_midnight"):
                        acc["cross_midnight_group_ids"].add(component_id)

        # Persist only detected cell-period combinations. All omitted cells are
        # implicitly UNKNOWN because FIRMS has no full swath/cloud mask.
        payload: list[dict[str, Any]] = []
        touched_cells = { (row_index, col_index) for _, row_index, col_index in accumulators }
        for (period_index, row_index, col_index), acc in sorted(accumulators.items()):
            projected_geometry = get_cell(row_index, col_index)
            if projected_geometry is None:
                continue
            geographic_geometry = transform_geometry(context.inverse.transform, projected_geometry)
            modis_count = acc["modis_count"]
            viirs_count = acc["viirs_count"]
            opportunity_summary = {
                "evidence_source": "FIRMS_DETECTION_ONLY",
                "MODIS": "DETECTED" if modis_count > 0 else "UNKNOWN",
                "VIIRS": "DETECTED" if viirs_count > 0 else "UNKNOWN",
                "valid_area_fraction": None,
                "note": FIRMS_OPPORTUNITY_NOTE,
                "weighted_count_semantics": "pixel-equivalent footprint allocation",
                "centroid_fallback_count": acc["centroid_fallback_count"],
                "allocation_note": (
                    "Observations without usable scan/track are assigned to the centre cell; "
                    "they are not treated as area-weighted footprints."
                ),
            }
            period_start, period_end = periods[period_index]
            payload.append({
                "processing_run_id": run_id,
                "grid_cell_id": f"LAEA:{grid_resolution_km:g}km:r{row_index}:c{col_index}",
                "grid_resolution_km": grid_resolution_km,
                "cell_geometry_wkt": geographic_geometry.wkt,
                "period_start": period_start,
                "period_end": period_end,
                "modis_count": modis_count,
                "viirs_count": viirs_count,
                "matched_group_count": len(acc["group_ids"]) if matching_run_id is not None else None,
                "modis_frp_sum_mw": acc["modis_frp_sum_mw"] if acc["has_modis_frp"] else None,
                "viirs_frp_sum_mw": acc["viirs_frp_sum_mw"] if acc["has_viirs_frp"] else None,
                "valid_area_fraction": None,
                "opportunity_summary": json.dumps(opportunity_summary, sort_keys=True, separators=(",", ":")),
                "activity_status": "DETECTED_ACTIVITY",
                "primary_calendar_day": min(acc["primary_calendar_days"]) if acc["primary_calendar_days"] else periods[period_index][0].date().isoformat(),
                "is_cross_midnight": bool(acc["cross_midnight_group_ids"]),
                "explanation_json": json.dumps({
                    "plain_language": (
                        f"{len(acc['cross_midnight_group_ids'])} cross-midnight candidate group(s) assigned once to "
                        f"{min(acc['primary_calendar_days'])}." if acc["cross_midnight_group_ids"] else
                        "Detection observations are allocated to this UTC calendar period; matched-group counts are assigned only to each component's primary UTC day."
                    ),
                    "assigned_candidate_group_ids": sorted(acc["group_ids"]),
                    "cross_midnight_candidate_group_ids": sorted(acc["cross_midnight_group_ids"]),
                    "primary_calendar_day": min(acc["primary_calendar_days"]) if acc["primary_calendar_days"] else periods[period_index][0].date().isoformat(),
                    "evidence_source": data_source_mode,
                }, sort_keys=True, separators=(",", ":")),
            })

        inserted = repository.insert_aggregation_results(payload)
        repository.finish_processing_run(
            run_id, status="COMPLETED", input_count=len(observations), output_count=inserted
        )
    except Exception as exc:
        repository.finish_processing_run(
            run_id, status="FAILED", input_count=0, output_count=0, error_message=str(exc)
        )
        raise

    return {
        "processing_run_id": run_id,
        "module_name": "AGGREGATION",
        "input_observations": len(observations),
        "grid_cells_with_detections": len(touched_cells),
        "grid_bbox_slots_upper_bound": grid_bbox_slots,
        "periods": len(periods),
        "output_count": inserted,
        "grid_resolution_km": grid_resolution_km,
        "storage_strategy": "SPARSE_DETECTED_CELL_PERIODS",
        "omitted_cell_period_status": "UNKNOWN",
        "valid_area_fraction": None,
        "run_cache_key": config["run_cache_key"],
        "selected_platforms": selected_platforms or ["ALL"],
        "data_source_mode": data_source_mode,
        "status": "COMPLETED",
    }


def _feature(geometry: Any, properties: Mapping[str, Any]) -> dict[str, Any]:
    safe_properties = {}
    def json_safe(value: Any) -> Any:
        if isinstance(value, datetime):
            return _utc(value).isoformat().replace("+00:00", "Z")
        if isinstance(value, dict):
            return {str(k): json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_safe(item) for item in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)
    for key, value in properties.items():
        safe_properties[key] = json_safe(value)
    return {"type": "Feature", "geometry": mapping(geometry), "properties": safe_properties}


def _observation_geojson_geometry(
    observation: Mapping[str, Any], context: EqualAreaContext,
    footprint_provider: FootprintProvider | None = None,
) -> tuple[Any, str]:
    provider_impl = footprint_provider or FIRMSScanTrackFootprintProvider()
    polygon = provider_impl.build_polygon(observation, context)
    if polygon is None:
        return Point(float(observation["longitude"]), float(observation["latitude"])), POINT_FALLBACK_GEOMETRY_SOURCE
    if isinstance(provider_impl, CompositeFootprintProvider):
        source = provider_impl.source_for(observation)
    else:
        source = provider_impl.geometry_source
    return transform_geometry(context.inverse.transform, polygon), source


def export_validation_geojson(
    repository: DatabaseRepository,
    *,
    processing_run_id: int,
    output_dir: str | Path,
    footprint_provider: FootprintProvider | None = None,
) -> dict[str, str]:
    """Export footprints, candidate links and common grid as RFC 7946 GeoJSON."""
    run = repository.get_processing_run(processing_run_id)
    if run is None:
        raise KeyError(
            f"Processing run {processing_run_id} does not exist in this DATABASE_URL. "
            "Use `python matching_and_measurement.py runs` to list actual run IDs. "
            "Note: --matching-run-id is an input association run; the grid command "
            "creates a separate AGGREGATION processing_run_id for GeoJSON export."
        )
    if run.get("status") != "COMPLETED":
        raise ValueError(
            f"Processing run {processing_run_id} has status {run.get('status')!r}; "
            "only COMPLETED runs can be exported. If it was terminated, rerun the "
            "grid command and export the new processing_run_id."
        )
    try:
        config = json.loads(run["configuration_json"])
    except (TypeError, json.JSONDecodeError):
        config = {}
    bbox_value = config.get("bbox")
    association_run_id = processing_run_id
    if run["module_name"] == "AGGREGATION" and config.get("matching_run_id") is not None:
        association_run_id = int(config["matching_run_id"])
    provider_impl = footprint_provider or CompositeFootprintProvider()
    expected_geometry_source = config.get("geometry_source")
    if expected_geometry_source and str(expected_geometry_source) != provider_impl.geometry_source:
        raise ValueError(
            "GeoJSON footprint export must use the same FootprintProvider as the selected run. "
            f"Run requires {expected_geometry_source!r}, supplied provider is {provider_impl.geometry_source!r}."
        )
    # For an aggregation run linked to a separate matching run, candidate-link
    # metrics are already persisted with their original geometry_source. The
    # footprint layer is regenerated using the aggregation run's own provider;
    # line links need no footprint-provider reconstruction.
    associations = repository.fetch_candidate_associations(association_run_id)
    measurement_rows = repository.fetch_measurement_results(processing_run_id) if run["module_name"] == "MEASUREMENT" else []
    aggregation_rows = repository.fetch_aggregation_results(processing_run_id)
    observation_ids: set[int] = set()
    for edge in associations:
        if edge.get("modis_observation_id") is not None:
            observation_ids.add(int(edge["modis_observation_id"]))
        if edge.get("viirs_observation_id") is not None:
            observation_ids.add(int(edge["viirs_observation_id"]))
    for measurement in measurement_rows:
        observation_ids.add(int(measurement["observation_id"]))

    if not observation_ids and bbox_value is not None and config.get("start_date") and config.get("end_date"):
        start_at, end_at, _, _ = _date_bounds(config["start_date"], config["end_date"])
        bbox_obj = BoundingBox.parse(bbox_value)
        candidates = repository.fetch_scientific_observations(
            sensors=("MODIS", "VIIRS"), start_at=start_at, end_at=end_at,
            west=bbox_obj.west, south=bbox_obj.south, east=bbox_obj.east, north=bbox_obj.north,
            qc_processing_run_id=config.get("qc_processing_run_id"),
            require_matching_suitability=False,
        )
        observation_ids.update(int(row["observation_id"]) for row in candidates)
    observations_by_id = {
        int(row["observation_id"]): row for row in repository.fetch_observations_by_ids(observation_ids)
    }
    if bbox_value is None:
        context_bbox = _derive_bbox(list(observations_by_id.values()))
    else:
        context_bbox = BoundingBox.parse(bbox_value)
    context = build_equal_area_context(context_bbox)
    footprints_features = []
    for observation_id in sorted(observations_by_id):
        row = observations_by_id[observation_id]
        geometry, geometry_source = _observation_geojson_geometry(row, context, provider_impl)
        footprints_features.append(_feature(geometry, {
            "observation_id": observation_id,
            "sensor": row.get("sensor"),
            "platform": row.get("satellite"),
            "acquired_at": row.get("acquired_at"),
            "frp_raw_mw": row.get("frp_mw"),
            "qc_status": row.get("qc_quality_status"),
            "geometry_source": geometry_source,
            "footprint_available": geometry_source != POINT_FALLBACK_GEOMETRY_SOURCE,
        }))

    links_features = []
    for edge in associations:
        m_id, v_id = edge.get("modis_observation_id"), edge.get("viirs_observation_id")
        if m_id is None or v_id is None or edge.get("relationship_status") in {"UNMATCHED", "REJECTED"}:
            continue
        modis = observations_by_id.get(int(m_id))
        viirs = observations_by_id.get(int(v_id))
        if not modis or not viirs:
            continue
        line = LineString([
            (float(modis["longitude"]), float(modis["latitude"])),
            (float(viirs["longitude"]), float(viirs["latitude"])),
        ])
        links_features.append(_feature(line, {
            "candidate_id": edge.get("candidate_id"),
            "modis_observation_id": int(m_id),
            "viirs_observation_id": int(v_id),
            "iou": edge.get("iou"),
            "smaller_footprint_overlap": edge.get("smaller_footprint_overlap"),
            "intersection_area_km2": edge.get("intersection_area_km2"),
            "boundary_distance_km": edge.get("boundary_distance_km"),
            "centroid_distance_km": edge.get("centroid_distance_km"),
            "time_difference_minutes": edge.get("time_difference_minutes"),
            "relationship_status": edge.get("relationship_status"),
            "matching_method": edge.get("matching_method"),
            "geometry_source": edge.get("geometry_source"),
        }))

    grid_features = []
    for row in aggregation_rows:
        geom = load_wkt(row["cell_geometry_wkt"])
        try:
            opportunity_summary_value = json.loads(row.get("opportunity_summary") or "{}")
        except (TypeError, json.JSONDecodeError):
            opportunity_summary_value = {"raw": row.get("opportunity_summary")}
        grid_features.append(_feature(geom, {
            "agg_id": row.get("agg_id"),
            "grid_cell_id": row.get("grid_cell_id"),
            "grid_resolution_km": row.get("grid_resolution_km"),
            "period_start": row.get("period_start"),
            "period_end": row.get("period_end"),
            "modis_count": row.get("modis_count"),
            "viirs_count": row.get("viirs_count"),
            "matched_group_count": row.get("matched_group_count"),
            # Do not add sensor-specific FRP measurements into one total:
            # their observation operators/retrieval characteristics differ.
            "frp_sum": {
                "MODIS_mw": row.get("modis_frp_sum_mw"),
                "VIIRS_mw": row.get("viirs_frp_sum_mw"),
            },
            "modis_frp_sum_mw": row.get("modis_frp_sum_mw"),
            "viirs_frp_sum_mw": row.get("viirs_frp_sum_mw"),
            "opportunity_summary": opportunity_summary_value,
            "activity_status": row.get("activity_status"),
            "valid_area_fraction": row.get("valid_area_fraction"),
            "primary_calendar_day": row.get("primary_calendar_day"),
            "is_cross_midnight": row.get("is_cross_midnight"),
            "explanation": json.loads(row.get("explanation_json") or "{}"),
        }))

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    documents = {
        "footprints.geojson": {"type": "FeatureCollection", "features": footprints_features},
        "candidate_links.geojson": {"type": "FeatureCollection", "features": links_features},
        "common_grid.geojson": {"type": "FeatureCollection", "features": grid_features},
    }
    paths: dict[str, str] = {}
    for filename, document in documents.items():
        path = out / filename
        path.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
        # Read-after-write validation protects exports consumed by QGIS/GEE.
        parsed = json.loads(path.read_text(encoding="utf-8"))
        if parsed.get("type") != "FeatureCollection" or not isinstance(parsed.get("features"), list):
            raise ValueError(f"Invalid GeoJSON FeatureCollection written to {path}")
        for feature in parsed["features"]:
            if feature.get("geometry") is not None:
                shape(feature["geometry"])
        paths[filename] = str(path)
    return paths


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SANYAM Module 6 matching, FRP analysis and common grid")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy URL; defaults to DATABASE_URL/SQLite")
    sub = parser.add_subparsers(dest="command", required=True)

    baseline = sub.add_parser("baseline", help="Baseline Haversine/time matching")
    baseline.add_argument("--start-date", required=True)
    baseline.add_argument("--end-date", required=True)
    baseline.add_argument("--bbox", default=None, help="west,south,east,north")
    baseline.add_argument("--max-distance-km", type=float, default=DEFAULT_BASELINE_DISTANCE_KM)
    baseline.add_argument("--max-time-minutes", type=float, default=DEFAULT_TIME_DIFFERENCE_MINUTES, help="legacy/custom temporal threshold")
    baseline.add_argument("--window-preset", type=int, choices=TEMPORAL_WINDOW_PRESETS, default=None)
    baseline.add_argument("--custom-window-minutes", type=float, default=None, help="custom temporal threshold 1..360")
    baseline.add_argument("--platforms", default=None, help="comma-separated Terra,Aqua,Suomi-NPP,NOAA-20,NOAA-21")
    baseline.add_argument("--data-source-mode", choices=("FIRMS", "LAADS_LEVEL2", "BOTH"), default="BOTH")
    baseline.add_argument("--qc-processing-run-id", type=int, default=None)
    baseline.add_argument("--grid-resolution-km", type=float, choices=(1.0, 2.0, 5.0), default=None)

    footprint = sub.add_parser("footprint", help="Approximate footprint-aware matching")
    footprint.add_argument("--bbox", required=True)
    footprint.add_argument("--start-date", required=True)
    footprint.add_argument("--end-date", required=True)
    footprint.add_argument("--max-time-minutes", type=float, default=DEFAULT_TIME_DIFFERENCE_MINUTES, help="legacy/custom temporal threshold")
    footprint.add_argument("--window-preset", type=int, choices=TEMPORAL_WINDOW_PRESETS, default=None)
    footprint.add_argument("--custom-window-minutes", type=float, default=None, help="custom temporal threshold 1..360")
    footprint.add_argument("--platforms", default=None, help="comma-separated platforms")
    footprint.add_argument("--data-source-mode", choices=("FIRMS", "LAADS_LEVEL2", "BOTH"), default="BOTH")
    footprint.add_argument("--max-boundary-km", type=float, default=DEFAULT_BOUNDARY_DISTANCE_KM)
    footprint.add_argument("--fallback-centroid-km", type=float, default=DEFAULT_FALLBACK_CENTROID_DISTANCE_KM)
    footprint.add_argument("--qc-processing-run-id", type=int, default=None)
    footprint.add_argument("--grid-resolution-km", type=float, choices=(1.0, 2.0, 5.0), default=None)

    frp = sub.add_parser("frp", help="Preserve raw FRP and calculate detection-limit diagnostics")
    frp.add_argument("--start-date", default=None)
    frp.add_argument("--end-date", default=None)
    frp.add_argument("--bbox", default=None)
    frp.add_argument("--qc-processing-run-id", type=int, default=None)

    grid = sub.add_parser("grid", help="Build equal-area common-grid activity summaries")
    grid.add_argument("--bbox", required=True)
    grid.add_argument("--start-date", required=True)
    grid.add_argument("--end-date", required=True)
    grid.add_argument("--resolution-km", type=float, choices=(1.0, 2.0, 5.0), default=1.0)
    grid.add_argument("--period-days", type=int, default=1)
    grid.add_argument("--matching-run-id", type=int, default=None)
    grid.add_argument("--qc-processing-run-id", type=int, default=None)
    grid.add_argument("--platforms", default=None, help="comma-separated Terra,Aqua,Suomi-NPP,NOAA-20,NOAA-21")
    grid.add_argument("--data-source-mode", choices=("FIRMS", "LAADS_LEVEL2", "BOTH"), default="BOTH")

    export = sub.add_parser("export", help="Export three GeoJSON layers for a processing run")
    export.add_argument("--processing-run-id", type=int, required=True)
    export.add_argument("--output-dir", default="data/exports/validation")

    runs = sub.add_parser("runs", help="List recent scientific processing runs")
    runs.add_argument("--limit", type=int, default=20)
    runs.add_argument("--module", choices=("QC", "OPPORTUNITY", "MATCHING", "MEASUREMENT", "AGGREGATION"), default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s | %(levelname)s | %(message)s")
    args = _build_parser().parse_args(argv)
    repository = connect_db(args.database_url)
    try:
        if args.command == "baseline":
            result = run_baseline_matching(
                repository, start_date=args.start_date, end_date=args.end_date, bbox=args.bbox,
                max_distance_km=args.max_distance_km,
                max_time_difference_minutes=args.max_time_minutes,
                qc_processing_run_id=args.qc_processing_run_id,
                platforms=[part.strip() for part in args.platforms.split(",") if part.strip()] if args.platforms else None,
                data_source_mode=args.data_source_mode,
                window_preset=args.window_preset,
                temporal_window_minutes=args.custom_window_minutes,
                grid_resolution_km=args.grid_resolution_km,
            )
        elif args.command == "footprint":
            result = run_footprint_matching(
                repository, bbox=args.bbox, start_date=args.start_date, end_date=args.end_date,
                max_time_difference_minutes=args.max_time_minutes,
                max_boundary_distance_km=args.max_boundary_km,
                fallback_centroid_distance_km=args.fallback_centroid_km,
                qc_processing_run_id=args.qc_processing_run_id,
                platforms=[part.strip() for part in args.platforms.split(",") if part.strip()] if args.platforms else None,
                data_source_mode=args.data_source_mode,
                window_preset=args.window_preset,
                temporal_window_minutes=args.custom_window_minutes,
                grid_resolution_km=args.grid_resolution_km,
            )
        elif args.command == "frp":
            result = analyze_frp_measurements(
                repository, start_date=args.start_date, end_date=args.end_date,
                bbox=args.bbox, qc_processing_run_id=args.qc_processing_run_id,
            )
        elif args.command == "grid":
            result = build_common_grid_summary(
                repository, bbox=args.bbox, start_date=args.start_date, end_date=args.end_date,
                grid_resolution_km=args.resolution_km, period_days=args.period_days,
                matching_run_id=args.matching_run_id, qc_processing_run_id=args.qc_processing_run_id,
                platforms=[part.strip() for part in args.platforms.split(",") if part.strip()] if args.platforms else None,
                data_source_mode=args.data_source_mode,
            )
        elif args.command == "export":
            result = export_validation_geojson(
                repository, processing_run_id=args.processing_run_id, output_dir=args.output_dir,
            )
        else:  # runs
            if args.limit < 1:
                raise ValueError("--limit must be >= 1")
            listed_runs = repository.list_processing_runs(limit=args.limit, module_name=args.module)
            result = {
                "runs": [
                    {key: (value.isoformat() if isinstance(value, datetime) else value)
                     for key, value in row.items()}
                    for row in listed_runs
                ]
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        LOG.exception("Module 6 operation failed")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
