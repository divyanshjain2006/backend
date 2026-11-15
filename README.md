# SANYAM — Satellite Active-fire Harmonization Backend

SANYAM is a research backend for preserving, quality-controlling, comparing and aggregating active-fire satellite observations from **NASA FIRMS CSV** and **NASA LAADS DAAC Level-2 Active Fire granules**.

The system is designed around scientific traceability: a satellite detection is an observation, not a confirmed physical fire event; raw source files are preserved; sensor-native values remain unchanged; candidate links are not forced one-to-one matches; and absence of a point detection is not interpreted as a valid non-detection unless a geolocated Level-2 swath supports that conclusion.

> **Implementation status:** this repository contains an integrated adapter, shared database, QC/opportunity, matching/measurement and FastAPI implementation. The automated tests use deterministic fixtures and mocked granule arrays. They do **not** establish that every LAADS file revision has been validated with a live Earthdata download. See [Scientific limitations and implementation boundaries](#scientific-limitations-and-implementation-boundaries) before interpreting outputs as validated scientific products.

---

## 1. Capabilities

### Data sources and products

| Source | Product | Platform | Format | Collection configured |
|---|---|---|---|---|
| FIRMS CSV | MODIS active fire | Terra / Aqua | CSV | FIRMS product feed |
| FIRMS CSV | VIIRS active fire | Suomi-NPP / NOAA-20 / NOAA-21 | CSV | FIRMS product feed |
| LAADS Level-2 | `MOD14` | Terra | HDF4 | `061` / C6.1 |
| LAADS Level-2 | `MYD14` | Aqua | HDF4 | `061` / C6.1 |
| LAADS Level-2 | `VNP14IMG` | Suomi-NPP | NetCDF4 / HDF5 | `002` / C2 |
| LAADS Level-2 | `VJ114IMG` | NOAA-20 | NetCDF4 / HDF5 | `002` / C2 |
| LAADS Level-2 | `VJ214IMG` | NOAA-21 | NetCDF4 / HDF5 | `002` / C2 |

The FIRMS ingestion implementation remains in `firms_ingest.py`. LAADS support is added in `laads_ingest.py`. Both adapters insert observations into the same repository schema.

### Platform selection

Supported canonical platform names:

- `Terra`
- `Aqua`
- `Suomi-NPP`
- `NOAA-20`
- `NOAA-21`

Matching can select a subset using `--platforms` or the API `platforms` array. Cross-sensor matching requires at least one MODIS platform and one VIIRS platform. The API exposes `/platforms` and `/diagnostics/platforms` so clients can show the platform catalogue and date-range warnings.

Operational availability checks are guardrails, not a replacement for NASA archive inventory. They warn or skip requests fully outside the date ranges encoded in `laads_ingest.py`. In particular, the current code records the Suomi-NPP science-data delivery cutoff as **2026-11-02 13:00 UTC**; check NASA/NOAA notices before planning future or retrospective coverage around a platform transition.

### Matching windows

Preset windows are **5, 15, 30, 60 and 120 minutes**. Custom windows from **1 through 360 minutes** are supported. The CLI accepts `--window-preset` or `--custom-window-minutes`; the API accepts `window_preset` or `custom_window_minutes` (not both).

When a matching threshold is greater than 60 minutes, candidate associations receive a `DIURNAL_DRIFT_WARNING`. The warning says that fire intensity and/or scene conditions may have changed during the longer time separation; it does not automatically reject the candidate.

### Cross-midnight behaviour

Matching compares timezone-aware UTC timestamps continuously. It does not split the candidate search by `YYYY-MM-DD` before calculating the time difference. For a query interval `[query_start, query_end]`, source-observation retrieval is padded by `±T_max`; pairs are retained when at least one member lies in the original unpadded query interval.

A group that spans UTC midnight receives `is_cross_midnight = true`. Its primary calendar day is assigned using the earliest acquisition timestamp in the candidate group. That day-assignment rule is used for calendar representation; it does not rewrite the native acquisition timestamps.

---

## 2. Architecture

```text
                  NASA FIRMS CSV                NASA LAADS DAAC Level-2
                         |                                  |
                   firms_ingest.py                    laads_ingest.py
                         |                       HDF4 / NetCDF4 / HDF5 readers
                         |                                  |
                         +------------------+---------------+
                                            |
                                    Shared db.py layer
                              SQLAlchemy Core / SQLite / PostgreSQL
                                            |
              +-----------------------------+--------------------------+
              |                             |                          |
        qc_and_opportunity.py       matching_and_measurement.py      api.py
          Module 4 + 5                Module 6 + grid             FastAPI service
              |                             |                          |
        qc_results              candidate_associations           Query endpoints
        observation_opportunities measurement_results              JSON responses
                                     aggregation_results             |
                                                              Swagger / ReDoc
```

### Main files

| File | Responsibility |
|---|---|
| `db.py` | Shared SQLAlchemy Core schema, SQLite/PostgreSQL connection, additive migrations and repository methods |
| `firms_ingest.py` | Existing FIRMS CSV ingestion, quota-aware/resumable downloads and source-row preservation |
| `laads_ingest.py` | LAADS archive search/download, immutable granule storage, HDF4/NetCDF4/HDF5 readers, mask decoding and Level-2 observation extraction |
| `qc_and_opportunity.py` | Observation QC, structured warnings, FIRMS `UNKNOWN` fallback and Level-2 swath opportunity evaluation |
| `matching_and_measurement.py` | Continuous UTC candidate matching, geometry-aware matching, FRP diagnostics, common-grid aggregation and GeoJSON export |
| `api.py` | FastAPI routes for ingestion, platform diagnostics, observations, QC, opportunity, matching, measurement, aggregation and export |
| `requirements.txt` | Runtime dependencies |
| `requirements-dev.txt` | Runtime dependencies plus pytest |
| `.env.example` | Non-secret environment template |
| `tests/` | Unit and API tests; LAADS granule readers are tested with mock arrays rather than live data downloads |

### Shared database entities

The shared database includes the scientific tables below (plus run/ingestion metadata tables used to track execution):

- `raw_assets` — immutable source-file metadata, path/URI, file size, SHA-256, granule/product/collection/platform and acquisition bounds.
- `observations` — normalized point records with native sensor values, source IDs, raw-asset links, mask/QA attributes and optional footprint WKT.
- `qc_results` — QC status, task-specific suitability and `warnings_json`; observations are not deleted when QC fails.
- `observation_opportunities` — evidence-backed coverage/detection classifications with area fractions and explanation/provenance JSON where available.
- `candidate_associations` — candidate relationships between MODIS and VIIRS observations; not physical fire-event identity.
- `measurement_results` — FRP source values and diagnostics/operator inputs.
- `aggregation_results` — sparse common-grid activity summaries and explainability fields.
- `processing_runs` / ingestion-run metadata — execution status, configuration and provenance.

Schema changes for Level-2 support are additive. Existing SQLite databases are migrated when the repository initializes. For a production PostgreSQL database, take a backup before deploying code that applies schema migrations.

---

## 3. Requirements and installation

### Python and runtime

Use Python 3.10+ (Python 3.11 or 3.12 is a good target for current scientific Python wheels). A virtual environment is strongly recommended.

```bash
cd /workspaces/experiment-1/sanyam_firms_portable
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

For development, install:

```bash
pip install -r requirements-dev.txt
```

### HDF4 system dependency

`pyhdf` may require the native HDF4 development/runtime libraries and a compatible compiler toolchain on Linux. If `pip install pyhdf` fails, install the HDF4 packages available for your distribution first, then retry the Python dependency installation. NetCDF4/HDF5 reading uses `netCDF4` and can fall back to `h5py` for HDF5 files.

### Environment variables

Copy the template and edit it locally:

```bash
cp .env.example .env
```

`.env`:

```dotenv
# Existing FIRMS CSV adapter
FIRMS_MAP_KEY=YOUR_FIRMS_MAP_KEY

# LAADS Level-2 archive/download client
EARTHDATA_TOKEN=YOUR_EARTHDATA_TOKEN

# Local development database
DATABASE_URL=sqlite:///data/sanyam_firms.sqlite3
LOG_LEVEL=INFO
```

Never commit `.env`, a real token, a real FIRMS MAP_KEY, downloaded source files or a private database. The LAADS token is read on the server/CLI from the process environment or `.env`; it is not accepted in API request bodies.

For PostgreSQL, use a SQLAlchemy URL, for example:

```dotenv
DATABASE_URL=postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/sanyam
```

URL-encode special characters in a database password. Provision the database and restrict the application user's permissions appropriately.

Create writable data directories:

```bash
mkdir -p data/raw/modis/terra data/raw/modis/aqua \
  data/raw/viirs/snpp data/raw/viirs/noaa20 data/raw/viirs/noaa21 \
  data/exports
```

Raw granule files are written under the platform-specific folders above. The application stores their absolute paths in `raw_assets`; keep the storage root available to the service process and back it up according to your research data policy.

---

## 4. First checks and automated tests

Run from the repository root with `.venv` activated:

```bash
# Confirm every top-level module imports.
python -c "import db, firms_ingest, laads_ingest, qc_and_opportunity, matching_and_measurement, api; print('ALL IMPORTS OK')"

# Compile all Python modules.
python -m compileall -q .

# Run the complete unittest suite.
python -m unittest discover -s tests -v
```

Individual test groups:

```bash
python -m unittest discover -s tests -p 'test_firms_ingest.py' -v
python -m unittest discover -s tests -p 'test_laads_ingest.py' -v
python -m unittest discover -s tests -p 'test_qc_and_opportunity.py' -v
python -m unittest discover -s tests -p 'test_matching_and_measurement.py' -v
python -m unittest discover -s tests -p 'test_api.py' -v
```

The current source snapshot was verified locally with `python -m compileall -q .` and **99 tests passing** using `python -m unittest discover -s tests -q`. This is the test status of the source snapshot used to prepare this README, not a claim that your separate Codespace has already run them. Live Earthdata/LAADS downloads are not part of the deterministic unit tests.

The tests exercise, among other things:

- FIRMS CSV ingestion and persistence regression coverage.
- Product/collection registry and LAADS authenticated request construction.
- Atomic `.part` download handling and refusal of URLs outside the configured LAADS host.
- Mock MODIS and VIIRS mask/fire-pixel arrays, mask decoding and source-attribute retention.
- Zero-based `FP_line`/`FP_sample` indexing for both MODIS C6.1 and VIIRS C2.
- True-corner polygon construction only when explicit pixel-corner arrays exist.
- Raw-asset SHA-256 metadata and writing into the common repository schema.
- QC warnings and Level-2 opportunity states, including cloud-obscured and valid no-detection cases.
- UTC midnight matching, temporal padding, run configuration keys, platform filters and temporal presets/custom windows.
- FastAPI route validation, JSON responses and missing-secret behaviour.

---

## 5. Run the existing FIRMS CSV pipeline

The original FIRMS adapter remains available and uses `FIRMS_MAP_KEY`. First check connectivity and quota status:

```bash
python firms_ingest.py availability
python firms_ingest.py status
```

A small Standard Processing historical test request:

```bash
python firms_ingest.py ingest \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only \
  --chunk-days 1
```

The bounding-box order is `west,south,east,north`. Use a small time window and a small region first; check the MAP_KEY transaction allowance before expanding the request.

Check ingestion status again:

```bash
python firms_ingest.py status
```

FIRMS CSV is detection-only. When no detection row exists, that feed cannot by itself distinguish a valid observation with no fire from cloud obstruction, an orbit gap or missing coverage. The FIRMS opportunity adapter therefore reports `UNKNOWN` rather than manufacturing `VALID_NO_DETECTION`.

---

## 6. Ingest NASA LAADS Level-2 granules

### Product selection

All five product names are supported by the configured registry. To ingest a subset, pass comma-separated product IDs.

```bash
python laads_ingest.py ingest \
  --products MOD14,MYD14,VNP14IMG,VJ114IMG,VJ214IMG \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30
```

For a smaller first test, select one product, e.g.:

```bash
python laads_ingest.py ingest \
  --products MOD14 \
  --start-date 2024-01-01 \
  --end-date 2024-01-02 \
  --bbox 73,26,78,30
```

The LAADS client uses the configured product collection and date/bbox query, authorizes with the Earthdata bearer token, streams downloads to a temporary `.part` file and atomically renames a completed file. Credentials are never embedded in source code.

### Raw archive layout

```text
data/raw/
├── modis/
│   ├── terra/       # MOD14 .hdf
│   └── aqua/        # MYD14 .hdf
└── viirs/
    ├── snpp/        # VNP14IMG NetCDF4/HDF5
    ├── noaa20/      # VJ114IMG NetCDF4/HDF5
    └── noaa21/      # VJ214IMG NetCDF4/HDF5
```

Existing destination granules are not downloaded over. `raw_assets` records the file path, file size, checksum, product, collection, granule ID, platform and acquisition bounds. Observations point back to the raw asset and preserve the extracted native attributes. The raw HDF/NetCDF file remains the authoritative source for the complete 2D mask and QA arrays.

### What is extracted

- MODIS fire-pixel table fields include line/sample, center latitude/longitude, FRP, T21/T31, confidence, adjacent cloud, solar/day-night fields where present, plus the full fire mask and algorithm QA.
- VIIRS fire-pixel table fields include line/sample, center latitude/longitude, FRP, T4/T5, background mean T4/T5, adjacent cloud, confidence, solar/day-night fields where present, plus the full fire mask and algorithm QA.
- Native `FP_line` and `FP_sample` positions are treated as **zero-based** for MODIS C6.1 and VIIRS C2, matching the NASA product guides.
- Pixel-level time is marked `LINEAR_INTERPOLATED_FROM_GRANULE_TIME_BOUNDS` when interpolated from the line position and granule start/end metadata. Where there are insufficient bounds it is marked `GRANULE_START_ONLY`; neither status should be presented as a native per-pixel UTC timestamp.

If the HDF4 reader fails to import `pyhdf`, install the required native HDF4 system libraries and reinstall `pyhdf`. Do not work around a decoder failure by silently treating the source file as a CSV.

---

## 7. Quality control and observation opportunity

### Run QC

After either FIRMS or LAADS observations are ingested:

```bash
python qc_and_opportunity.py qc
```

The QC layer preserves source rows and writes a result/status plus structured warning objects to `qc_results.warnings_json`. Examples include low confidence, adjacent cloud contamination, VIIRS bow-tie/glint indicators, non-positive footprint dimensions and Level-2 timestamp/geometry caveats. QC is task-specific: an observation may be suitable for spatial/temporal matching while unsuitable for FRP measurement.

### FIRMS detection-only opportunity

```bash
python qc_and_opportunity.py opportunity \
  --sensor MODIS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --evidence-mode FIRMS

python qc_and_opportunity.py opportunity \
  --sensor VIIRS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --evidence-mode FIRMS
```

### Level-2 swath opportunity

```bash
python qc_and_opportunity.py opportunity \
  --sensor MODIS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --platforms Terra,Aqua \
  --evidence-mode LEVEL2
```

For VIIRS:

```bash
python qc_and_opportunity.py opportunity \
  --sensor VIIRS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --platforms Suomi-NPP,NOAA-20,NOAA-21 \
  --evidence-mode LEVEL2
```

The Level-2 provider can classify a requested bbox only when it can relate the mask to explicit geolocated pixel corners and time bounds. Its supported state vocabulary is:

- `DETECTED` — one or more qualifying fire-mask pixels intersect the request.
- `VALID_NO_DETECTION` — valid observed area meets the configured fraction and no fire pixels occur in that area.
- `CLOUD_OBSCURED` — cloud-covered area dominates the valid-observation evidence.
- `INVALID_QUALITY` — intersecting pixels are present but quality prevents a valid interpretation.
- `NOT_COVERED` — no valid geolocated pixel footprint intersects the request.
- `UNKNOWN` — the data cannot establish coverage or observation state.

`beta_ret` is the valid observed fraction of the query area in this Level-2 bbox assessment. It is not filled in when the geometry/time evidence is missing. The current common-grid aggregator does **not** yet compute this fraction independently for every grid cell; see [Known limitations](#scientific-limitations-and-implementation-boundaries).

---

## 8. Matching and temporal controls

### Baseline Haversine matching

```bash
python matching_and_measurement.py baseline \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --max-distance-km 2.0 \
  --window-preset 30 \
  --platforms Terra,Aqua,Suomi-NPP,NOAA-20,NOAA-21 \
  --data-source-mode BOTH
```

### Footprint-aware matching

```bash
python matching_and_measurement.py footprint \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --window-preset 30 \
  --platforms Terra,NOAA-20 \
  --data-source-mode BOTH \
  --max-boundary-km 0.25
```

### Custom time window

For a custom 45-minute search:

```bash
python matching_and_measurement.py baseline \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --custom-window-minutes 45 \
  --platforms Terra,NOAA-20 \
  --data-source-mode LAADS_LEVEL2
```

Use only one of `--window-preset` and `--custom-window-minutes`. The custom range is 1–360 minutes. Windows longer than 60 minutes add a `DIURNAL_DRIFT_WARNING` to candidate explanations.

### Midnight-crossing regression example

For example, a MODIS observation at `2026-11-12T23:55:00Z` and VIIRS observation at `2026-11-13T00:05:00Z` have a continuous UTC difference of 10 minutes. With `T_max=30`, the fetch range is padded by 30 minutes at each query boundary, and the pair can be considered even though the timestamps fall on different UTC calendar dates.

### Match output semantics

- Candidate links preserve many-to-many relationships; a detection is not forced into a single match.
- The matching run records selected platforms, selected temporal window, spatial method, source mode and grid-resolution context in its configuration/run key.
- Candidate explanations include a plain-language summary and evidence/provenance fields where available.
- A candidate association is not a confirmed physical fire event.

---

## 9. FRP diagnostics, common grid and export

### FRP diagnostic run

```bash
python matching_and_measurement.py frp \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30
```

Original sensor FRP values remain unchanged. The code stores diagnostic inputs such as native brightness/background values and day/night context when supplied, and exposes an extension hook for a future observation operator. It does not claim that a generic cross-sensor multiplier harmonizes MODIS and VIIRS FRP.

### Common-grid aggregation

Use a spatial resolution of 1, 2 or 5 km and a positive aggregation period. To associate the grid run with a specific matching run, supply its ID:

```bash
python matching_and_measurement.py grid \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --resolution-km 1.0 \
  --period-days 1 \
  --matching-run-id 5 \
  --platforms Terra,NOAA-20 \
  --data-source-mode BOTH
```

`--matching-run-id` is optional. If omitted, the aggregation run will not have an explicit matching-run relationship; it will be recorded with a null matching-run ID. The grid is sparse and stores detected/touched cell-periods rather than allocating every possible cell over the full bounding box. Omitted cells should not be read as verified no-fire cells.

### Find run IDs

```bash
python matching_and_measurement.py runs --module MATCHING --limit 20
python matching_and_measurement.py runs --module AGGREGATION --limit 20
```

### Export GeoJSON

Export using the **aggregation** processing-run ID, not the matching run ID:

```bash
python matching_and_measurement.py export \
  --processing-run-id 7 \
  --output-dir data/exports/validation/run_7
```

Check the generated files:

```bash
find data/exports/validation/run_7 -maxdepth 2 -type f -print
```

An aggregation run created without `--matching-run-id` can export its available grid layer, but it will not be linked to that matching run's candidate-association links. For a traceable export containing both aggregation and matching-derived layers, rerun aggregation with the intended `--matching-run-id`, then export the new aggregation run ID.

---

## 10. FastAPI service and query interfaces

### Start the service

Make sure `.env` is present and the database is reachable:

```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

Open:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
- Health: `http://localhost:8000/health`

In GitHub Codespaces, use the forwarded port link for port `8000`.

### Endpoint index

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Database/service health |
| `GET` | `/platforms` | Platform catalogue and temporal window options |
| `GET` | `/diagnostics/platforms` | Selected platform date-range diagnostics |
| `POST` | `/ingestion/laads/run` | Search/download/ingest LAADS products |
| `GET` | `/raw-assets` | Query raw granule assets |
| `GET` | `/observations` | Query observations by sensor, date, bbox and platform |
| `POST` | `/qc/run` | Run QC |
| `GET` | `/qc/{processing_run_id}` | Retrieve QC warnings/results |
| `POST` | `/opportunities/run` | Run FIRMS or Level-2 opportunity classification |
| `GET` | `/opportunities/summary` | Query opportunity state counts |
| `GET` | `/opportunities/{processing_run_id}` | Retrieve opportunity evidence and explanations |
| `POST` | `/measurement/frp/run` | Run FRP diagnostics |
| `GET` | `/measurement/{processing_run_id}` | Retrieve measurement diagnostics |
| `POST` | `/matching/run` | Run baseline or footprint matching |
| `GET` | `/matching/{processing_run_id}/candidates` | Retrieve candidate associations and explanations |
| `GET` | `/processing-runs` | List processing runs |
| `GET` | `/processing-runs/{processing_run_id}` | Retrieve run configuration/status |
| `POST` | `/aggregation/run` | Build common-grid aggregation |
| `GET` | `/aggregation/{processing_run_id}` | Retrieve aggregated cells |
| `POST` | `/aggregation/{processing_run_id}/export` | Export GeoJSON for an aggregation run |

### Example: LAADS ingestion via API

```bash
curl -X POST http://localhost:8000/ingestion/laads/run \
  -H 'Content-Type: application/json' \
  -d '{
    "products": ["MOD14", "VNP14IMG"],
    "start_date": "2024-01-01",
    "end_date": "2024-01-02",
    "bbox": [73, 26, 78, 30]
  }'
```

The API reads `EARTHDATA_TOKEN` from the server environment. Do not send credentials in this JSON.

### Example: platform diagnostics

```bash
curl -G 'http://localhost:8000/diagnostics/platforms' \
  --data-urlencode 'platforms=NOAA-21' \
  --data-urlencode 'start_date=2022-01-01' \
  --data-urlencode 'end_date=2022-01-05'
```

### Example: run matching with a custom window

```bash
curl -X POST http://localhost:8000/matching/run \
  -H 'Content-Type: application/json' \
  -d '{
    "method": "FOOTPRINT_AWARE",
    "start_date": "2026-11-12",
    "end_date": "2026-11-13",
    "bbox": [73, 26, 78, 30],
    "sensors": ["MODIS", "VIIRS"],
    "platforms": ["Terra", "NOAA-20"],
    "custom_window_minutes": 45,
    "data_source_mode": "BOTH",
    "grid_resolution_km": 1.0
  }'
```

### Example: run Level-2 opportunity via API

```bash
curl -X POST http://localhost:8000/opportunities/run \
  -H 'Content-Type: application/json' \
  -d '{
    "sensor": "VIIRS",
    "bbox": [73, 26, 78, 30],
    "start_date": "2024-01-01",
    "end_date": "2024-01-05",
    "platforms": ["NOAA-20"],
    "evidence_mode": "LEVEL2"
  }'
```

### Example: query observations

```bash
curl -G 'http://localhost:8000/observations' \
  --data-urlencode 'sensor=MODIS' \
  --data-urlencode 'start_date=2024-01-01' \
  --data-urlencode 'end_date=2024-01-05' \
  --data-urlencode 'bbox=73' \
  --data-urlencode 'bbox=26' \
  --data-urlencode 'bbox=78' \
  --data-urlencode 'bbox=30' \
  --data-urlencode 'platforms=Terra'
```

All API date-only values are interpreted as inclusive UTC calendar dates. The API docs (`/docs`) expose the request models and validation rules.

---

## 11. Explainability and provenance

The service serializes JSON explanation fields rather than returning only a numeric score. Depending on result type and the evidence available, explanations can include:

- Plain-English candidate, disagreement or opportunity reason.
- Sensor/platform/product and collection.
- Observation IDs, granule ID or FIRMS source record/path.
- Raw-asset ID, SHA-256 and source storage path when available.
- Geometry provenance (`LEVEL2_PIXEL_CORNERS`, `FIRMS_APPROX_SCAN_TRACK`, or center-only/unavailable state).
- Evidence category, temporal separation and whether the association spans UTC midnight.
- Temporal-window warnings, including the diurnal drift warning for longer windows.
- Opportunity evidence source, valid/cloud/invalid observed-area fractions and reasons where the geometry supports those quantities.

An explanation is not a calibrated probability. It should be read as a traceable description of the evidence and decision rule that produced the output.

---

## 12. Scientific limitations and implementation boundaries

These boundaries are intentional. The application should not fill absent evidence with plausible-looking estimates.

1. **No automatic MOD03/MYD03/VNP03IMG pairing yet.** The current reader builds `LEVEL2_PIXEL_CORNERS` only when explicit corner arrays are available to the adapter. The standard fire-pixel latitude/longitude values are pixel centres. They are not relabelled as four-corner geometry. Automatic companion geolocation acquisition/join (MOD03/MYD03 and VIIRS geolocation products) is a separate integration task.
2. **Pixel timestamps are not native per-pixel UTC timestamps.** Where start/end bounds and scan-line index are available, time is linearly interpolated and labelled as such. Otherwise the granule start is used and explicitly labelled `GRANULE_START_ONLY`.
3. **Swath opportunity requires real pixel corners and time bounds.** If these are unavailable, the provider must return `UNKNOWN` or fail to make a no-detection claim. A mask with no geolocation cannot prove that a requested bbox was covered.
4. **Grid-cell opportunity is not yet a full per-cell Level-2 mask join.** The bbox swath opportunity function calculates `beta_ret` only for that assessed query bbox when explicit corners exist. The common-grid aggregation does not yet compute an independently validated `beta_ret` for every grid cell; `valid_area_fraction` is therefore left null rather than invented.
5. **The full Sofiev & Kouznetsov (2026) FRP observation operator is not implemented.** This version preserves native FRP and supplies diagnostic/operator extension inputs and a hook. It does not claim full physical harmonization or a universal sensor conversion factor. Do not interpret the diagnostic as harmonized FRP.
6. **No physical fire identity is inferred.** Matching creates many-to-many candidate associations between satellite observations. One source pixel may have multiple candidates, and an association does not establish that two sensors observed the same physical fire.
7. **Mask classes and QA must be interpreted by product collection.** MODIS C6.1 and VIIRS C2 differ. Their class numbers/bit meanings must not be mixed. VIIRS residual bow-tie QA is important for avoiding redundant FRP counts.
8. **Real-data validation is still required.** Validate one known granule per product/platform in a controlled environment and compare selected mask pixels, coordinates, acquisition metadata and QA fields with the NASA product guide and file specification before large-scale ingestion.
9. **LAADS search and token use must be checked at runtime.** This repository has mocked tests for request construction/download handling, not a claim that the live archive returned data for every requested product/date/bbox.
10. **Operational availability checks are not an orbit predictor.** The diagnostics warn on coarse date ranges; they do not compute exact local overpass opportunities or certify coverage.

### Recommended next scientific/engineering increments

1. Pair the fire granules with their product-specific geolocation companion granules and map pixel centres to official four-corner polygons, preserving the geolocation source file's checksum and granule linkage.
2. Validate scale/offset/fill values, dataset naming, granule metadata and mask/QA decoding against one authentic file from each of the five products.
3. Build a time-resolved swath-footprint index so Level-2 opportunity can be evaluated for each aggregation grid cell without loading every granule into memory at once.
4. Pass measured pixel area, day/night and background temperature fields into a scientifically validated, versioned FRP observation operator; keep original native FRP values immutable.
5. Add integration tests that use tiny, redistributable real granule subsets alongside the current mock-array tests.

---

## 13. Troubleshooting

### `ModuleNotFoundError: No module named 'sanyam_firms_portable'`

The current project layout uses top-level Python modules. Imports should look like:

```python
from db import DatabaseRepository, connect_db
```

not `from sanyam_firms_portable.db import ...`. Search for stale imports:

```bash
grep -RIn 'sanyam_firms_portable' --include='*.py' .
```

### `EARTHDATA_TOKEN is not set`

Check that `.env` is in the project root, the variable name is exactly `EARTHDATA_TOKEN`, and the process was restarted after editing it. Never paste a token into source code or a chat.

### `pyhdf` cannot import or find the HDF4 library

Install the system HDF4 development/runtime package for your Linux distribution, then reinstall `pyhdf` inside the active virtual environment. Confirm `python -c 'from pyhdf.SD import SD; print("pyhdf OK")'`.

### API says database unavailable

Check `DATABASE_URL`, database credentials/network access and that the user can create/use the required tables. For a local SQLite test, the project must be run from the repository root so `data/sanyam_firms.sqlite3` resolves as expected.

### LAADS request finds no granules

Check product ID, collection, UTC date range, bbox order (`west,south,east,north`), token permissions and whether that product/platform delivered science granules for the period. A successful HTTP response containing no matching file is not itself an ingestion error.

### Export has no matching candidate links

Use the aggregation run ID with the export command. If you need matching candidate links associated with the grid output, rerun `grid` with `--matching-run-id <MATCHING_RUN_ID>`; an aggregation run created with a null matching-run ID is not retroactively linked.

### Inspect all processing runs

```bash
python matching_and_measurement.py runs --limit 50
```

Use `--module QC`, `OPPORTUNITY`, `MATCHING`, `MEASUREMENT` or `AGGREGATION` to filter.

---

## 14. Source documentation

Use the collection-specific official guides when checking native values and algorithm semantics:

- [NASA MODIS Collection 6 / 6.1 Active Fire Product User Guide](https://www.earthdata.nasa.gov/s3fs-public/2023-09/MODIS_C6_C6.1_Fire_User_Guide_1.0.pdf)
- [NASA VIIRS Collection 2 375 m Active Fire Product User Guide](https://ladsweb.modaps.eosdis.nasa.gov/archive/Document%20Archive/Science%20Data%20Product%20Documentation/VIIRS_C2_AF-375m_User_Guide_1.2.pdf)
- [NASA LAADS DAAC](https://ladsweb.modaps.eosdis.nasa.gov/)
- [LAADS DAAC API documentation](https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/)
- [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/)

When a product guide and this README differ, the product's official collection-specific file specification and user guide take precedence. Update the decoder and its tests together; do not silently apply one product's QA interpretation to another sensor or collection.

---

## 15. Research-use disclaimer

SANYAM is a research and engineering system, not an operational fire-warning service. Before publishing a scientific conclusion, preserve the selected source collections, filters, platform set, time threshold, processing run IDs, code version, raw asset checksums, geometry source and QC/opportunity evidence. Report uncertainty and avoid describing unmatched observations as proof that no fire existed.
