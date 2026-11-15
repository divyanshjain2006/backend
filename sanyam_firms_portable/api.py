#!/usr/bin/env python3
"""SANYAM FastAPI service for ingestion, opportunity, matching and query APIs.

Run with:
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload

Secrets (EARTHDATA_TOKEN, FIRMS_MAP_KEY) are read only from the server
environment/.env and are never accepted in request bodies or returned.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from dotenv import load_dotenv

load_dotenv()

from db import DatabaseRepository, connect_db
from laads_ingest import (
    LAADSClient,
    PRODUCTS,
    canonical_platform,
    ingest_laads,
    platform_availability_diagnostics,
)
from matching_and_measurement import (
    build_common_grid_summary,
    export_validation_geojson,
    run_baseline_matching,
    run_footprint_matching,
    analyze_frp_measurements,
)
from qc_and_opportunity import (
    Level2SwathOpportunityProvider,
    evaluate_observation_opportunity,
    query_opportunity_summary,
    run_quality_control,
)

ALLOWED_GRID_RESOLUTIONS_KM = (1.0, 2.0, 5.0)


def _validate_grid_resolution(value: float | None) -> float | None:
    """Restrict grid resolutions to the scientifically supported presets."""
    if value is None:
        return None
    numeric = float(value)
    if numeric not in ALLOWED_GRID_RESOLUTIONS_KM:
        allowed = ", ".join(str(item) for item in ALLOWED_GRID_RESOLUTIONS_KM)
        raise ValueError(f"grid_resolution_km must be one of: {allowed}")
    return numeric


PLATFORM_DETAILS: dict[str, dict[str, Any]] = {
    "Terra": {"sensor": "MODIS", "product": "MOD14", "collection": "61", "active_from": "2000-02-24", "active_to": None},
    "Aqua": {"sensor": "MODIS", "product": "MYD14", "collection": "61", "active_from": "2002-07-04", "active_to": None},
    "Suomi-NPP": {"sensor": "VIIRS", "product": "VNP14IMG", "collection": "002", "active_from": "2012-01-19", "active_to": "2026-11-02T13:00:00Z"},
    "NOAA-20": {"sensor": "VIIRS", "product": "VJ114IMG", "collection": "002", "active_from": "2018-01-01", "active_to": None},
    "NOAA-21": {"sensor": "VIIRS", "product": "VJ214IMG", "collection": "002", "active_from": "2022-11-01", "active_to": None},
}


def _utc_day_bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    return (
        datetime.combine(start_date, time.min, tzinfo=timezone.utc),
        datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc),
    )


def _safe_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return default


def _serialize_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["configuration"] = _safe_json(result.pop("configuration_json", None), {})
    return result


def _serialize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["explanation"] = _safe_json(result.pop("explanation_json", None), {})
    result["configuration"] = _safe_json(result.pop("config_json", None), {})
    return result


def _serialize_opportunity(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["explanation"] = _safe_json(result.pop("explanation_json", None), {})
    result["granule_ids"] = _safe_json(result.pop("granule_ids_json", None), [])
    return result


class QCRunRequest(BaseModel):
    method_version: str = "1.0.0"
    batch_size: int = Field(default=1000, ge=1, le=10000)
    reprocess: bool = False


class FRPMeasurementRequest(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    qc_processing_run_id: int | None = None

    @model_validator(mode="after")
    def validate_request(self):
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("start_date and end_date must be supplied together")
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if self.bbox is not None:
            west, south, east, north = self.bbox
            if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
                raise ValueError("invalid bbox")
        return self


class LAADSIngestRequest(BaseModel):
    products: list[Literal["MOD14", "MYD14", "VNP14IMG", "VJ114IMG", "VJ214IMG"]] = Field(default_factory=lambda: list(PRODUCTS))
    start_date: date
    end_date: date
    bbox: list[float] = Field(min_length=4, max_length=4)
    raw_root: str = "data/raw"

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value: list[float]) -> list[float]:
        west, south, east, north = value
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("bbox must be west,south,east,north within geographic bounds")
        return value

    @model_validator(mode="after")
    def validate_dates(self):
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self


class OpportunityRunRequest(BaseModel):
    sensor: Literal["MODIS", "VIIRS"]
    bbox: list[float] = Field(min_length=4, max_length=4)
    start_date: date
    end_date: date
    platforms: list[str] | None = None
    evidence_mode: Literal["FIRMS", "LEVEL2"] = "FIRMS"
    qc_processing_run_id: int | None = None

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value: list[float]) -> list[float]:
        west, south, east, north = value
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("invalid bbox")
        return value

    @model_validator(mode="after")
    def validate_dates(self):
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if self.platforms:
            self.platforms = sorted({canonical_platform(x) for x in self.platforms})
            selected_sensors = {PLATFORM_DETAILS[x]["sensor"] for x in self.platforms}
            if selected_sensors != {self.sensor}:
                raise ValueError(f"opportunity sensor={self.sensor} requires platforms from that sensor family only")
        return self


class MatchingRunRequest(BaseModel):
    method: Literal["BASELINE_HAVERSINE", "FOOTPRINT_AWARE"] = "FOOTPRINT_AWARE"
    start_date: date
    end_date: date
    bbox: list[float] = Field(min_length=4, max_length=4)
    sensors: list[Literal["MODIS", "VIIRS"]] = Field(default_factory=lambda: ["MODIS", "VIIRS"])
    platforms: list[str] | None = None
    window_preset: Literal[5, 15, 30, 60, 120] | None = None
    custom_window_minutes: float | None = Field(default=None, ge=1, le=360)
    max_distance_km: float = Field(default=2.0, gt=0)
    max_boundary_distance_km: float = Field(default=0.25, ge=0)
    data_source_mode: Literal["FIRMS", "LAADS_LEVEL2", "BOTH"] = "BOTH"
    grid_resolution_km: float | None = Field(default=None, ge=1.0, le=5.0)
    qc_processing_run_id: int | None = None

    @field_validator("grid_resolution_km")
    @classmethod
    def validate_grid_resolution(cls, value: float | None) -> float | None:
        return _validate_grid_resolution(value)

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value: list[float]) -> list[float]:
        west, south, east, north = value
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("invalid bbox")
        return value

    @model_validator(mode="after")
    def validate_request(self):
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if set(self.sensors) != {"MODIS", "VIIRS"}:
            raise ValueError("cross-sensor matching requires sensors=['MODIS','VIIRS']; select platforms to control which satellites participate")
        if self.window_preset is not None and self.custom_window_minutes is not None:
            raise ValueError("choose either window_preset or custom_window_minutes")
        if self.platforms:
            self.platforms = sorted({canonical_platform(x) for x in self.platforms})
            selected_sensors = {PLATFORM_DETAILS[x]["sensor"] for x in self.platforms}
            if selected_sensors != {"MODIS", "VIIRS"}:
                raise ValueError("select at least one MODIS platform and one VIIRS platform for cross-sensor matching")
        return self


class AggregationRunRequest(BaseModel):
    bbox: list[float] = Field(min_length=4, max_length=4)
    start_date: date
    end_date: date
    grid_resolution_km: float = Field(default=1.0, ge=1.0, le=5.0)
    period_days: int = Field(default=1, ge=1, le=366)
    matching_run_id: int | None = None
    platforms: list[str] | None = None
    data_source_mode: Literal["FIRMS", "LAADS_LEVEL2", "BOTH"] = "BOTH"

    @field_validator("grid_resolution_km")
    @classmethod
    def validate_grid_resolution(cls, value: float) -> float:
        return float(_validate_grid_resolution(value))

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value: list[float]) -> list[float]:
        west, south, east, north = value
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("invalid bbox")
        return value

    @model_validator(mode="after")
    def validate_request(self):
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        if self.platforms:
            self.platforms = sorted({canonical_platform(x) for x in self.platforms})
        return self


def create_app(repository_factory: Callable[..., DatabaseRepository] = connect_db) -> FastAPI:
    app = FastAPI(
        title="SANYAM Active-Fire Harmonization API",
        version="0.3.0",
        description=(
            "Research service for FIRMS CSV and LAADS Level-2 active-fire observations. "
            "Outputs are satellite observations and candidate associations, not confirmed physical fire events."
        ),
    )

    def repository_dependency():
        database_url = os.getenv("DATABASE_URL")
        repository = repository_factory(database_url)
        try:
            yield repository
        finally:
            repository.close()

    from fastapi import Depends

    @app.get("/health")
    def health(repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            count = repository.count_observations()
            return {"status": "ok", "database": repository.display_target, "observation_count": count}
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"database health check failed: {exc}") from exc

    @app.get("/platforms")
    def platforms():
        return {
            "platforms": [
                {"platform": name, **details}
                for name, details in PLATFORM_DETAILS.items()
            ],
            "temporal_window_presets_minutes": [5, 15, 30, 60, 120],
            "custom_temporal_window_minutes": {"minimum": 1, "maximum": 360},
        }

    @app.get("/diagnostics/platforms")
    def platform_diagnostics(
        platforms: list[str] = Query(..., description="Terra,Aqua,Suomi-NPP,NOAA-20,NOAA-21"),
        start_date: date = Query(...),
        end_date: date = Query(...),
    ):
        try:
            start_at, end_at = _utc_day_bounds(start_date, end_date)
            selected = sorted({canonical_platform(value) for value in platforms})
            return {"selected_platforms": selected, "diagnostics": platform_availability_diagnostics(selected, start_at, end_at)}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/qc/run")
    def run_qc(request: QCRunRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            return run_quality_control(
                repository, method_version=request.method_version,
                batch_size=request.batch_size, reprocess=request.reprocess,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/qc/{processing_run_id}")
    def qc_results(processing_run_id: int, repository: DatabaseRepository = Depends(repository_dependency)):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "QC":
            raise HTTPException(status_code=422, detail="processing run is not a QC run")
        records = repository.fetch_qc_results(processing_run_id)
        for record in records:
            record["warnings"] = _safe_json(record.pop("warnings_json", None), [])
        return {"processing_run": _serialize_run(run), "count": len(records), "records": records}

    @app.post("/ingestion/laads/run")
    def run_laads_ingestion(request: LAADSIngestRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        token = os.getenv("EARTHDATA_TOKEN")
        if not token:
            raise HTTPException(status_code=503, detail="EARTHDATA_TOKEN is not configured on the server")
        try:
            return ingest_laads(
                repository,
                client=LAADSClient(token),
                products=request.products,
                start_date=request.start_date,
                end_date=request.end_date,
                bbox=request.bbox,
                raw_root=Path(request.raw_root),
            )
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/raw-assets")
    def raw_assets(
        start_date: date | None = None,
        end_date: date | None = None,
        platforms: list[str] | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=2000),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        try:
            if start_date and end_date and end_date < start_date:
                raise ValueError("end_date must be on or after start_date")
            start_at = datetime.combine(start_date, time.min, tzinfo=timezone.utc) if start_date else None
            end_at = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc) if end_date else None
            selected = [canonical_platform(x) for x in platforms] if platforms else None
            rows = repository.fetch_raw_assets(start_at=start_at, end_at=end_at, platforms=selected, limit=limit)
            return {"count": len(rows), "raw_assets": rows}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/observations")
    def observations(
        sensor: Literal["MODIS", "VIIRS"],
        start_date: date,
        end_date: date,
        bbox: list[float] = Query(..., min_length=4, max_length=4),
        platforms: list[str] | None = Query(default=None),
        limit: int = Query(default=500, ge=1, le=5000),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        try:
            west, south, east, north = bbox
            start_at, end_at = _utc_day_bounds(start_date, end_date)
            rows = repository.fetch_observations_for_opportunity(
                sensor=sensor, west=west, south=south, east=east, north=north,
                start_at=start_at, end_at=end_at,
            )
            if platforms:
                from qc_and_opportunity import _platform_name
                allowed = {canonical_platform(x) for x in platforms}
                rows = [r for r in rows if _platform_name(sensor, r.get("satellite")) in allowed]
            rows = rows[:limit]
            for row in rows:
                row["native_attributes"] = _safe_json(row.pop("native_attributes_json", None), {})
                row["raw_record"] = _safe_json(row.pop("raw_json", None), {})
                row["qc_warnings"] = _safe_json(row.pop("qc_warnings_json", None), [])
            return {"count": len(rows), "observations": rows}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/opportunities/run")
    def run_opportunity(request: OpportunityRunRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            start_at, end_at = _utc_day_bounds(request.start_date, request.end_date)
            provider = None
            source_mode = "FIRMS"
            if request.evidence_mode == "LEVEL2":
                from laads_ingest import iter_level2_swaths_for_opportunity
                provider = Level2SwathOpportunityProvider(
                    iter_level2_swaths_for_opportunity(
                        repository, sensor=request.sensor, start_at=start_at, end_at=end_at,
                        bbox=request.bbox, platforms=request.platforms,
                    )
                )
                source_mode = "LAADS_LEVEL2"
            return evaluate_observation_opportunity(
                repository, bbox=request.bbox, start_date=request.start_date, end_date=request.end_date,
                sensor=request.sensor, qc_processing_run_id=request.qc_processing_run_id,
                platforms=request.platforms, provider=provider, data_source_mode=source_mode,
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/opportunities/summary")
    def opportunity_summary(
        sensor: Literal["MODIS", "VIIRS"],
        start_date: date,
        end_date: date,
        bbox: list[float] = Query(..., min_length=4, max_length=4),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        try:
            state_counts = query_opportunity_summary(
                repository, bbox=bbox, start_date=start_date, end_date=end_date, sensor=sensor,
            )
            return {"sensor": sensor, "start_date": start_date, "end_date": end_date, "states": state_counts}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/opportunities/{processing_run_id}")
    def opportunity_records(processing_run_id: int, repository: DatabaseRepository = Depends(repository_dependency)):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "OPPORTUNITY":
            raise HTTPException(status_code=422, detail="processing run is not an OPPORTUNITY run")
        records = repository.fetch_opportunities(processing_run_id)
        return {"processing_run": _serialize_run(run), "count": len(records),
                "records": [_serialize_opportunity(r) for r in records]}

    @app.post("/measurement/frp/run")
    def run_frp_measurement(request: FRPMeasurementRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            return analyze_frp_measurements(
                repository, start_date=request.start_date, end_date=request.end_date,
                bbox=request.bbox, qc_processing_run_id=request.qc_processing_run_id,
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/measurement/{processing_run_id}")
    def measurement_results(processing_run_id: int, repository: DatabaseRepository = Depends(repository_dependency)):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "MEASUREMENT":
            raise HTTPException(status_code=422, detail="processing run is not a MEASUREMENT run")
        records = repository.fetch_measurement_results(processing_run_id)
        for record in records:
            record["operator_inputs"] = _safe_json(record.pop("operator_inputs_json", None), {})
            record["diagnostic"] = _safe_json(record.pop("diagnostic_json", None), {})
        return {"processing_run": _serialize_run(run), "count": len(records), "records": records}

    @app.post("/matching/run")
    def run_matching(request: MatchingRunRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            common = {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "bbox": request.bbox,
                "platforms": request.platforms,
                "data_source_mode": request.data_source_mode,
                "qc_processing_run_id": request.qc_processing_run_id,
                "window_preset": request.window_preset,
                "temporal_window_minutes": request.custom_window_minutes,
                "grid_resolution_km": request.grid_resolution_km,
            }
            if request.method == "BASELINE_HAVERSINE":
                result = run_baseline_matching(
                    repository, **common, max_distance_km=request.max_distance_km,
                )
            else:
                result = run_footprint_matching(
                    repository, **common,
                    max_boundary_distance_km=request.max_boundary_distance_km,
                )
            return result
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/matching/{processing_run_id}/candidates")
    def matching_candidates(
        processing_run_id: int,
        limit: int = Query(default=1000, ge=1, le=10000),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "MATCHING":
            raise HTTPException(status_code=422, detail="processing run is not a MATCHING run")
        rows = repository.fetch_candidate_associations(processing_run_id)[:limit]
        return {"processing_run": _serialize_run(run), "count": len(rows), "records": [_serialize_candidate(r) for r in rows]}

    @app.get("/processing-runs")
    def processing_runs(
        module: Literal["QC", "OPPORTUNITY", "MATCHING", "MEASUREMENT", "AGGREGATION"] | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        rows = repository.list_processing_runs(limit=limit, module_name=module)
        return {"count": len(rows), "runs": [_serialize_run(r) for r in rows]}

    @app.get("/processing-runs/{processing_run_id}")
    def processing_run(processing_run_id: int, repository: DatabaseRepository = Depends(repository_dependency)):
        row = repository.get_processing_run(processing_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        return _serialize_run(row)

    @app.post("/aggregation/run")
    def run_aggregation(request: AggregationRunRequest, repository: DatabaseRepository = Depends(repository_dependency)):
        try:
            return build_common_grid_summary(
                repository,
                bbox=request.bbox,
                start_date=request.start_date,
                end_date=request.end_date,
                grid_resolution_km=request.grid_resolution_km,
                period_days=request.period_days,
                matching_run_id=request.matching_run_id,
                platforms=request.platforms,
                data_source_mode=request.data_source_mode,
            )
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/aggregation/{processing_run_id}")
    def aggregation_results(
        processing_run_id: int,
        limit: int = Query(default=5000, ge=1, le=20000),
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "AGGREGATION":
            raise HTTPException(status_code=422, detail="processing run is not an AGGREGATION run")
        rows = repository.fetch_aggregation_results(processing_run_id)[:limit]
        return {"processing_run": _serialize_run(run), "count": len(rows), "cells": [_serialize_opportunity(r) for r in rows]}

    @app.post("/aggregation/{processing_run_id}/export")
    def export_aggregation(
        processing_run_id: int,
        output_dir: str = "data/exports/api",
        repository: DatabaseRepository = Depends(repository_dependency),
    ):
        run = repository.get_processing_run(processing_run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="processing run not found")
        if run["module_name"] != "AGGREGATION":
            raise HTTPException(status_code=422, detail="processing run is not an AGGREGATION run")
        try:
            files = export_validation_geojson(repository, processing_run_id=processing_run_id, output_dir=Path(output_dir))
            return {"processing_run_id": processing_run_id, "files": files}
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app


app = create_app()
