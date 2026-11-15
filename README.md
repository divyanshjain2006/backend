# SANYAM — FIRMS Ingestion, Quality Control, Matching and Measurement Backend

A portable Python backend for ingesting NASA FIRMS active-fire observations, preserving source provenance, evaluating observation quality, constructing conservative observation-opportunity records, comparing MODIS and VIIRS detections, calculating FRP diagnostics, and creating common-grid GeoJSON outputs.

This repository is the **FIRMS-first backend milestone**. It uses SQLAlchemy Core so the same application code can run with SQLite for local development or PostgreSQL for a deployed environment.

> **Scientific boundary:** A FIRMS hotspot record is a satellite observation, not a confirmed physical fire event. FIRMS CSV is a detection-oriented feed; an empty FIRMS result cannot prove that an area was observed without fire. The backend preserves these distinctions in its data model and outputs.

---

## Contents

- [1. Current scope](#1-current-scope)
- [2. Architecture and project files](#2-architecture-and-project-files)
- [3. Requirements](#3-requirements)
- [4. Installation and configuration](#4-installation-and-configuration)
- [5. Database configuration](#5-database-configuration)
- [6. First-time verification](#6-first-time-verification)
- [7. Automated test suite](#7-automated-test-suite)
- [8. Complete end-to-end workflow](#8-complete-end-to-end-workflow)
- [9. Command reference](#9-command-reference)
- [10. Processing runs and run IDs](#10-processing-runs-and-run-ids)
- [11. GeoJSON exports](#11-geojson-exports)
- [12. Database tables and provenance](#12-database-tables-and-provenance)
- [13. Scientific assumptions and limitations](#13-scientific-assumptions-and-limitations)
- [14. Troubleshooting](#14-troubleshooting)
- [15. Recommended research workflow](#15-recommended-research-workflow)

---

## 1. Current scope

### Implemented in this backend

| Area | Current capability |
|---|---|
| FIRMS ingestion | Checks dataset availability and MAP_KEY status; downloads historical Area API CSV data |
| Processing selection | Supports Standard Processing (SP), automatic SP-first selection, and explicitly controlled NRT use |
| Provenance | Saves original CSV snapshots, SHA-256 checksums, source rows, dataset identifiers and ingestion-run metadata |
| Database | SQLite and PostgreSQL through a shared SQLAlchemy Core repository |
| Deduplication | Uses a stable source-row hash to prevent duplicate source records |
| Module 4 — QC | Validates observation fields and stores immutable, versioned quality results |
| Module 5 — Opportunity | Stores conservative observation-opportunity states from FIRMS detection evidence |
| Module 6 — Matching | Haversine/time baseline matching and approximate footprint-aware candidate matching |
| Module 6 — FRP | Preserves raw FRP and calculates preliminary detection-limit diagnostics |
| Module 6 — Grid | Builds sparse, equal-area common-grid activity summaries at 1, 2 or 5 km resolution |
| Export | Produces three validated GeoJSON FeatureCollections for GIS inspection |

### Not implemented in this milestone

- Direct NASA LAADS DAAC Level-2 HDF/NetCDF ingestion.
- Full MODIS/VIIRS Level-2 pixel-corner geolocation and quality-mask decoding.
- Swath-based proof of valid non-detection, cloud obstruction or orbital non-coverage.
- A fully implemented and scientifically validated cross-sensor FRP observation operator.
- Direct Google Earth Engine publishing or a frontend dashboard.

The code includes extension points for future Level-2 footprint, opportunity and FRP providers. Those interfaces do not mean that Level-2 processing has already been implemented.

---

## 2. Architecture and project files

The repository uses a **flat Python module layout**. Run commands from the repository root, where `db.py` is located.

```text
sanyam_firms_portable/
├── .env.example
├── .env                         # Create locally; do not commit
├── README.md
├── requirements.txt
├── db.py                        # SQLAlchemy Core schema and repository
├── firms_ingest.py              # NASA FIRMS API, ingestion and provenance
├── qc_and_opportunity.py        # Module 4 QC and Module 5 opportunity
├── matching_and_measurement.py  # Module 6 matching, FRP, grid and export
├── data/
│   ├── sanyam_firms.sqlite3     # Default SQLite database (created at runtime)
│   ├── raw/firms/               # Original FIRMS CSV snapshots
│   └── exports/                 # Generated GeoJSON outputs
└── tests/
    ├── test_firms_ingest.py
    ├── test_qc_and_opportunity.py
    └── test_matching_and_measurement.py
```

### Module responsibilities

**`db.py`**

- Defines the SQLAlchemy Core tables and indexes.
- Provides `DatabaseRepository` and `connect_db()`.
- Handles SQLite/PostgreSQL dialect differences, inserts, upserts and UTC-aware timestamps.
- Applies the supported additive SQLite schema migrations when the repository is initialized.

**`firms_ingest.py`**

- Reads `FIRMS_MAP_KEY` from the environment or `.env`.
- Queries FIRMS data availability and MAP_KEY transaction status.
- Plans source/date windows and downloads CSV in chunks of up to five days.
- Prefers SP and supports NRT fallback only under the documented automatic mode; manually selected NRT requires an explicit safety flag.
- Saves raw CSV snapshots and SHA-256 metadata, normalizes source fields, and stores original source values.

**`qc_and_opportunity.py`**

- Runs Module 4 quality checks and writes versioned QC results.
- Runs Module 5 opportunity summaries for MODIS or VIIRS over a requested bounding box and inclusive UTC date range.
- Uses `UNKNOWN` where FIRMS-only evidence cannot establish whether an area was validly observed without a detection.

**`matching_and_measurement.py`**

- Runs baseline point-distance/time candidate matching.
- Runs approximate scan/track footprint-aware candidate matching.
- Produces FRP diagnostic results without silently changing the source FRP.
- Builds sparse common-grid activity summaries.
- Lists processing runs and exports GeoJSON.

### Important Python import convention

Because the files are in the project root, imports between them must use:

```python
from db import DatabaseRepository, connect_db
```

Do not use `from sanyam_firms_portable.db import ...` unless the repository is deliberately converted into a separately installed Python package. This project is currently intended to be run as a flat set of modules from its root directory.

---

## 3. Requirements

- Python 3.10 or newer is recommended.
- A NASA FIRMS MAP_KEY for live FIRMS API requests.
- Internet access for live FIRMS API requests.
- SQLite (included with standard Python builds) for local development, or a reachable PostgreSQL server for deployment.

Python dependencies are listed in `requirements.txt`:

- `requests`
- `python-dotenv`
- `SQLAlchemy`
- `psycopg[binary]`
- `pyproj`
- `shapely`

`pyproj` and `shapely` are used by the projected geometry, footprint and GeoJSON workflows.

---

## 4. Installation and configuration

### Linux, macOS or GitHub Codespaces

Open a terminal at the repository root and run:

```bash
python --version
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
mkdir -p data/raw/firms data/exports
```

If the virtual environment already exists, activate it rather than creating another one:

```bash
source .venv/bin/activate
```

### Windows PowerShell

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
New-Item -ItemType Directory -Force data\raw\firms, data\exports
```

### Configure `.env`

Edit `.env` in the repository root:

```env
# NASA FIRMS MAP_KEY — replace the placeholder with your own key.
FIRMS_MAP_KEY=YOUR_ACTUAL_FIRMS_MAP_KEY

# Local SQLite database. This is the default development configuration.
DATABASE_URL=sqlite:///data/sanyam_firms.sqlite3

LOG_LEVEL=INFO
```

The scripts load `.env` through `python-dotenv`. Environment variables set in the shell can also be used. Do not put a real MAP_KEY in source code, a public repository, screenshots, issue reports or shared logs. Do not commit `.env`.

### Verify that the environment is ready

```bash
python -c "import requests, sqlalchemy, pyproj, shapely; print('Dependencies import successfully')"
```

---

## 5. Database configuration

### SQLite — local development

The default URL is:

```text
sqlite:///data/sanyam_firms.sqlite3
```

The database file is created when a command first initializes the repository. SQLite-specific connection settings are applied only to SQLite connections.

### PostgreSQL — deployment or shared testing

The project includes Psycopg 3 and supports SQLAlchemy PostgreSQL URLs. Configure `.env` with a URL in this form:

```env
DATABASE_URL=postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DATABASE_NAME
```

Replace the placeholders with the actual database credentials and endpoint. URL-encode reserved characters in the password. Keep production credentials in deployment secrets, not in Git.

The application code uses the same repository interface for SQLite and PostgreSQL. The automated suite compiles PostgreSQL DDL and upsert statements, but it does **not** prove that a live PostgreSQL server is reachable or that deployment credentials work. A real PostgreSQL connection test must be performed against a test database before production use.

### Override the database for a single command

The QC and Module 6 scripts accept a global `--database-url` option **before** the subcommand:

```bash
python qc_and_opportunity.py \
  --database-url "postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DATABASE_NAME" \
  qc

python matching_and_measurement.py \
  --database-url "postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DATABASE_NAME" \
  runs --limit 20
```

The ingestion command accepts `--database-url` as an option to `ingest`:

```bash
python firms_ingest.py ingest \
  --database-url "postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DATABASE_NAME" \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only
```

Back up an existing database before applying migrations or running a new version against important research data.

---

## 6. First-time verification

Run these commands from the repository root with `.venv` activated.

### A. Confirm the flat-module imports

```bash
python -c "import db, firms_ingest, qc_and_opportunity, matching_and_measurement; print('ALL MODULE IMPORTS SUCCESSFUL')"
```

Expected message:

```text
ALL MODULE IMPORTS SUCCESSFUL
```

### B. Compile all Python files

```bash
python -m compileall -q .
```

No output normally means compilation completed without syntax errors.

### C. Run all automated tests

```bash
python -m unittest discover -s tests -v
```

The source snapshot used to prepare this README passed **72 tests**. Re-run the command in the target checkout; the result can differ if files or dependencies have changed.

### D. Inspect CLI help

```bash
python firms_ingest.py --help
python firms_ingest.py ingest --help

python qc_and_opportunity.py --help
python qc_and_opportunity.py qc --help
python qc_and_opportunity.py opportunity --help
python qc_and_opportunity.py summary --help

python matching_and_measurement.py --help
python matching_and_measurement.py baseline --help
python matching_and_measurement.py footprint --help
python matching_and_measurement.py frp --help
python matching_and_measurement.py grid --help
python matching_and_measurement.py export --help
python matching_and_measurement.py runs --help
```

---

## 7. Automated test suite

### Run all tests

```bash
python -m unittest discover -s tests -v
```

### Run one test module at a time

```bash
python -m unittest discover -s tests -p "test_firms_ingest.py" -v
python -m unittest discover -s tests -p "test_qc_and_opportunity.py" -v
python -m unittest discover -s tests -p "test_matching_and_measurement.py" -v
```

### What the tests cover

| Test file | Main coverage |
|---|---|
| `test_firms_ingest.py` | CSV parsing and normalization; retention of native fields; stable row hashing and deduplication; raw snapshot SHA-256; SQLite persistence; resume behavior; quota-headroom logic; status parsing; additive legacy SQLite migration; PostgreSQL DDL and upsert compilation |
| `test_qc_and_opportunity.py` | Coordinate/time validation; MODIS numeric and VIIRS categorical confidence handling; QC warnings and invalid states; missing/nonpositive FRP behavior; NRT warning; immutable/reprocess behavior; FIRMS opportunity states; conservative `UNKNOWN` behavior; future Level-2 provider extension behavior; PostgreSQL DDL compilation |
| `test_matching_and_measurement.py` | Haversine distance; QC filtering; many-to-many associations; graph degrees and ambiguous components; approximate footprint geometry; overlap and boundary metrics; point fallback; FRP raw-value retention and diagnostics; equal-area grids; weighted allocation; sparse grid persistence; run linkage; GeoJSON export; PostgreSQL DDL compilation |

### What the automated suite does not test

- It does not perform a live NASA FIRMS download or validate a live MAP_KEY.
- It does not test your network connection or current FIRMS transaction quota.
- It does not connect to a live PostgreSQL instance. PostgreSQL-specific SQL is compiled in tests, but server authentication, permissions, migrations and runtime behavior need a separate integration test.
- It does not validate scientific accuracy against a ground-truth fire inventory.
- It does not test a full LAADS Level-2 HDF/NetCDF ingestion pipeline, because that pipeline is outside this milestone.

A passing unit-test suite is a software verification result, not proof that a scientific method is accurate for every region, sensor, collection or time period.

---

## 8. Complete end-to-end workflow

The following is a small smoke-test workflow for **1–5 January 2024** and the exploratory bounding box **73°E–78°E, 26°N–30°N**. It is a sample test area, not a validated final study region.

Run each stage in order. If a stage fails, stop, retain the full terminal error and resolve it before proceeding.

### Step 1 — Check FIRMS dataset availability

```bash
python firms_ingest.py availability
```

This prints the date availability advertised by FIRMS for the supported datasets. Dates are UTC/GMT.

### Step 2 — Check MAP_KEY transaction status

```bash
python firms_ingest.py status
```

Check that the key is accepted and has sufficient transaction headroom before starting a download.

### Step 3 — Ingest a small historical sample

```bash
python firms_ingest.py ingest \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only \
  --chunk-days 1
```

Important details:

- The date range is inclusive and interpreted as UTC.
- Bounding-box order is `west,south,east,north`.
- `--mode sp-only` prevents NRT fallback and is appropriate for a strict historical SP-only sample.
- The automatic default mode prefers SP and uses NRT only for dates where SP is unavailable. NRT observations remain labelled provisional. Manually selecting an NRT source requires `--allow-nrt`.
- The default chunk is one day; supported chunk sizes are 1–5 days.
- Original CSV snapshots are saved under `data/raw/firms/` by default. Do not use `--no-snapshots` for a provenance-sensitive research run unless there is a deliberate reason.
- Ingestion checks transaction headroom before starting additional requests. The default reserve is 1,000 transactions.

To resume after a quota pause or an interruption, repeat the same request and add `--resume`:

```bash
python firms_ingest.py ingest \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only \
  --chunk-days 1 \
  --resume
```

Resume skips exact dataset/date/bounding-box chunks already marked `COMPLETED`. Without `--resume`, the command can fetch the same source interval again; exact duplicate source rows are deduplicated by their stable source-row hash.

Optional: choose only selected sensor families by repeating `--family`:

```bash
python firms_ingest.py ingest \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only \
  --family MODIS \
  --family VIIRS_SNPP
```

Supported family values are `MODIS`, `VIIRS_SNPP`, `VIIRS_NOAA20` and `VIIRS_NOAA21`.

### Step 4 — Run Module 4 Quality Control

```bash
python qc_and_opportunity.py qc
```

The command processes observations that have not yet been evaluated by the selected QC method version. It creates a processing run and stores QC results; it does not delete source observations.

To deliberately evaluate all observations again and create a new QC result set:

```bash
python qc_and_opportunity.py qc --method-version 1.0.1 --reprocess
```

Use a new method version when the QC rules change. Reprocessing creates new results rather than overwriting old QC history.

### Step 5 — Run Module 5 Observation Opportunity

For MODIS:

```bash
python qc_and_opportunity.py opportunity \
  --sensor MODIS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05
```

For VIIRS:

```bash
python qc_and_opportunity.py opportunity \
  --sensor VIIRS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05
```

Query the most recent stored opportunity summary for MODIS or VIIRS:

```bash
python qc_and_opportunity.py summary \
  --sensor MODIS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05
```

Change `--sensor MODIS` to `--sensor VIIRS` for the VIIRS summary.

The FIRMS provider can identify a valid detection, a detection that failed QC, or an unknown opportunity state. It cannot infer a valid non-detection from an empty FIRMS CSV.

### Step 6 — Run Module 6 baseline matching

```bash
python matching_and_measurement.py baseline \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --max-distance-km 2.0 \
  --max-time-minutes 30
```

This uses a Haversine distance between observation coordinates plus a time-difference constraint. The 2 km and 30 minute values are exploratory starting parameters, not universal validated constants.

### Step 7 — Run approximate footprint-aware matching

```bash
python matching_and_measurement.py footprint \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --max-time-minutes 30 \
  --max-boundary-km 0.25
```

This uses reconstructed scan/track proxy geometries when usable dimensions are present. It is not a true Level-2 pixel-corner method. See [Scientific assumptions and limitations](#13-scientific-assumptions-and-limitations).

### Step 8 — Run FRP diagnostics

```bash
python matching_and_measurement.py frp \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30
```

The command preserves source FRP and writes preliminary detection-limit diagnostics. Do not interpret the diagnostic estimate as a complete sensor harmonization operator.

### Step 9 — Build the common grid

**Recommended linked version:** include the matching run ID that should be referenced by the aggregation run. For example, after running footprint matching, list runs and note its `MATCHING` run ID:

```bash
python matching_and_measurement.py runs --module MATCHING --limit 20
```

Then run the grid. Replace `FOOTPRINT_MATCHING_RUN_ID` with the actual matching run ID from your database:

```bash
python matching_and_measurement.py grid \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --resolution-km 1.0 \
  --period-days 1 \
  --matching-run-id FOOTPRINT_MATCHING_RUN_ID
```

Supported resolutions are `1.0`, `2.0` and `5.0` km. The default period is one day. The grid command creates a **new AGGREGATION processing run** and prints its own `processing_run_id`.

The `--matching-run-id` parameter is an optional reference to a previous `MATCHING` run. It is not the ID of the newly created aggregation run. Supplying it is recommended when the exported candidate links should correspond to a particular matching result.

If you omit `--matching-run-id`, the aggregation still runs, but the aggregation run has no explicit matching-run reference. An export for that aggregation run can therefore have an empty `candidate_links.geojson` layer even though separate matching runs exist in the database.

### Step 10 — Inspect processing runs

```bash
python matching_and_measurement.py runs --module MATCHING --limit 20
python matching_and_measurement.py runs --module AGGREGATION --limit 10
```

Only use a run with status `COMPLETED` for export. Note the `processing_run_id` from the `AGGREGATION` entry.

### Step 11 — Export GeoJSON

Replace `AGGREGATION_RUN_ID` with the ID created by Step 9 and shown by Step 10:

```bash
python matching_and_measurement.py export \
  --processing-run-id AGGREGATION_RUN_ID \
  --output-dir data/exports/validation/run_AGGREGATION_RUN_ID
```

Verify the files:

```bash
find data/exports/validation/run_AGGREGATION_RUN_ID -maxdepth 2 -type f -print
```

Expected filenames:

```text
footprints.geojson
candidate_links.geojson
common_grid.geojson
```

---

## 9. Command reference

Run the commands from the repository root. Add `--help` to any command for the current argument list.

### 9.1 FIRMS ingestion — `firms_ingest.py`

| Command | Purpose |
|---|---|
| `python firms_ingest.py availability` | Lists FIRMS dataset date availability |
| `python firms_ingest.py status` | Checks MAP_KEY transaction status |
| `python firms_ingest.py ingest ...` | Downloads and stores observations |

`ingest` options:

| Option | Meaning |
|---|---|
| `--start-date YYYY-MM-DD` | Required inclusive UTC start date |
| `--end-date YYYY-MM-DD` | Required inclusive UTC end date |
| `--bbox west,south,east,north` | Required bounding box; `world` is also accepted |
| `--source DATASET_ID` | Explicit dataset; may be repeated. Manual NRT requires `--allow-nrt` |
| `--mode auto` | Default: prefer SP, use NRT only where SP is unavailable |
| `--mode sp-only` | Use SP only; no NRT fallback |
| `--family FAMILY` | Limit automatic planning to selected family; may be repeated |
| `--allow-nrt` | Explicit safety switch for an NRT source |
| `--chunk-days 1..5` | Days in each FIRMS request; default is 1 |
| `--transaction-reserve N` | Leave a transaction buffer; default is 1,000 |
| `--resume` | Skip exact completed dataset/date/bbox chunks |
| `--database-url URL` | Override database URL for this ingestion |
| `--db PATH` | Deprecated SQLite-only path shorthand |
| `--raw-dir PATH` | Raw CSV snapshot directory; default `data/raw/firms` |
| `--no-snapshots` | Disables raw CSV snapshots; not recommended for research |

Supported dataset IDs include `MODIS_SP`, `MODIS_NRT`, `VIIRS_SNPP_SP`, `VIIRS_SNPP_NRT`, `VIIRS_NOAA20_SP`, `VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_SP` and `VIIRS_NOAA21_NRT`.

### 9.2 QC and opportunity — `qc_and_opportunity.py`

| Command | Purpose |
|---|---|
| `python qc_and_opportunity.py qc` | Runs Module 4 QC on observations not yet processed for the selected method version |
| `python qc_and_opportunity.py opportunity ...` | Runs Module 5 opportunity evaluation for a sensor, bbox and date range |
| `python qc_and_opportunity.py summary ...` | Queries the latest stored opportunity summary |

Common opportunity/summary arguments:

- `--sensor MODIS` or `--sensor VIIRS`
- `--bbox west,south,east,north`
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

Additional `qc` arguments:

- `--method-version VERSION`
- `--batch-size N`
- `--reprocess`

Additional `opportunity` arguments:

- `--qc-processing-run-id ID` to pin a particular QC result run
- `--platform PLATFORM`
- `--method-version VERSION`

The global `--database-url URL`, when needed, goes before the subcommand.

### 9.3 Module 6 — `matching_and_measurement.py`

| Subcommand | Purpose |
|---|---|
| `baseline` | Point-distance and time-window candidate matching |
| `footprint` | Approximate scan/track footprint-aware candidate matching |
| `frp` | Raw FRP preservation and detection-limit diagnostics |
| `grid` | Sparse common-grid aggregation |
| `export` | Writes three GeoJSON layers for a processing run |
| `runs` | Lists recent processing runs |

Important options:

| Subcommand | Options |
|---|---|
| `baseline` | `--start-date`, `--end-date`, optional `--bbox`, `--max-distance-km`, `--max-time-minutes`, `--qc-processing-run-id` |
| `footprint` | `--bbox`, `--start-date`, `--end-date`, `--max-time-minutes`, `--max-boundary-km`, `--fallback-centroid-km`, `--qc-processing-run-id` |
| `frp` | Optional `--start-date`, `--end-date`, `--bbox`, `--qc-processing-run-id` |
| `grid` | `--bbox`, `--start-date`, `--end-date`, `--resolution-km {1.0,2.0,5.0}`, `--period-days`, `--matching-run-id`, `--qc-processing-run-id` |
| `export` | Required `--processing-run-id`, optional `--output-dir` |
| `runs` | Optional `--module {QC,OPPORTUNITY,MATCHING,MEASUREMENT,AGGREGATION}`, `--limit` |

The global `--database-url URL`, when needed, goes before the subcommand.

---

## 10. Processing runs and run IDs

The backend stores scientific processing runs in `processing_runs`. A run records fields such as:

- `processing_run_id`
- `module_name`
- `method_version`
- `configuration_json`
- `started_at` and `completed_at`
- `input_count` and `output_count`
- `status` and `error_message`

Module names include `QC`, `OPPORTUNITY`, `MATCHING`, `MEASUREMENT` and `AGGREGATION`.

### How to find the right run ID

```bash
python matching_and_measurement.py runs --module MATCHING --limit 20
python matching_and_measurement.py runs --module AGGREGATION --limit 20
```

### Do not confuse these two IDs

- `--matching-run-id` is the ID of an existing `MATCHING` run used as an input/reference by grid aggregation.
- `--processing-run-id` in the export command is the ID of the completed run to export. For the common-grid workflow, this is the new `AGGREGATION` run ID printed by the grid command.

For a traceable grid-to-matching export, first select a completed matching run, pass it to `grid --matching-run-id`, then export the newly generated aggregation run.

Run IDs are database-specific and are generated at runtime. Do not copy example IDs from this README into a different database.

---

## 11. GeoJSON exports

The export command writes three RFC 7946-style WGS84 GeoJSON FeatureCollections and validates the files after writing.

### `footprints.geojson`

Observation geometries with properties such as observation ID, sensor, platform, acquisition time, raw FRP, QC status and geometry source. The current FIRMS scan/track rectangle is explicitly approximate. Rows without usable scan/track dimensions may use a labelled point fallback.

### `candidate_links.geojson`

LineStrings between MODIS and VIIRS observation centroids for non-rejected candidate associations. Properties include candidate ID, observation IDs, overlap/distance/time metrics, relationship status and matching method. A matching run must be linked to an aggregation run for these links to be selected through the aggregation export workflow.

### `common_grid.geojson`

Grid-cell polygons and properties such as resolution, period, sensor-specific weighted counts, candidate-group count, FRP allocation, opportunity summary, activity status and valid-area fraction.

FRP is represented separately for MODIS and VIIRS. The exporter does not create an unsupported single combined cross-sensor FRP total.

### Open in QGIS

1. Open QGIS.
2. Choose **Layer → Add Layer → Add Vector Layer**.
3. Browse to one or more exported `.geojson` files.
4. Add the layers and inspect their attribute tables.
5. Style `footprints`, `candidate_links` and `common_grid` independently.

The GeoJSON files can also be used as inputs to a later Google Earth Engine workflow. This backend does not automatically upload or publish them to Earth Engine.

---

## 12. Database tables and provenance

The repository defines the following main tables:

| Table | Purpose |
|---|---|
| `ingestion_runs` | Source dataset, requested date range, bbox, ingestion status and row counts |
| `raw_assets` | Original source-file reference, media type, byte size and SHA-256 checksum |
| `observations` | Normalized observation fields plus retained native source values and raw JSON |
| `processing_runs` | Versioned QC, opportunity, matching, measurement and aggregation run metadata |
| `qc_results` | Per-observation QC status, suitability flags, warnings and confidence category |
| `observation_opportunities` | Sensor/platform, spatial/time unit, opportunity state and evidence source |
| `candidate_associations` | MODIS–VIIRS candidate links and graph relationship information |
| `measurement_results` | FRP raw/diagnostic/adjusted fields and measurement provenance |
| `aggregation_results` | Grid-cell and time-period activity summaries |

### Provenance behavior

- Original FIRMS CSV snapshots are retained under the raw-data directory unless snapshots are explicitly disabled.
- SHA-256 checksums identify retained source assets.
- Exact source rows are deduplicated with a stable hash.
- `raw_json` retains the source row values; normalized fields do not replace the original evidence.
- Acquisition, ingestion and processing timestamps are handled as UTC-aware values.
- Processing runs retain configuration and method version so that outputs can be associated with the parameters used to create them.
- QC results are retained as processing outputs; the workflow does not delete source observations when they fail QC.

The SQLite migrations are additive for the supported older schema versions. Back up research data before upgrading or changing database backends.

---

## 13. Scientific assumptions and limitations

This section is essential when interpreting results or describing the backend in a NASA Space Apps presentation or research document.

### 13.1 SP and NRT are not interchangeable

Standard Processing (SP) is preferred for historical research runs. Near Real-Time (NRT) is provisional and must remain distinguishable. Use `--mode sp-only` when the analysis must exclude NRT. If NRT is used, report it clearly and do not silently mix it with SP as if both had identical maturity.

### 13.2 A detection is not a physical fire event

A MODIS or VIIRS FIRMS row represents a sensor observation. Candidate associations do not prove that two observations are the same physical fire. The matching output must not be described as a fire-event inventory without an independent event-identification method and validation.

### 13.3 FIRMS-only opportunity is conservative

FIRMS active-fire CSV provides detections, not the complete per-pixel swath, cloud and quality masks needed to prove valid observation without detection. Therefore:

- A valid FIRMS detection can be recorded as `DETECTED`.
- A QC-failed detection can be retained as `INVALID_QUALITY`.
- An area/time query with no detection remains `UNKNOWN`.
- The FIRMS-only workflow must not infer `VALID_NO_DETECTION`, `CLOUD_OBSCURED` or `NOT_COVERED` from an empty result.

A future Level-2 provider can support additional opportunity states only when it has the required coverage, swath and quality evidence.

### 13.4 Confidence values remain sensor-native

MODIS confidence is numeric; VIIRS confidence is represented by categorical labels. The QC module interprets these according to sensor-specific rules while retaining the native source value. Do not treat the original MODIS and VIIRS confidence fields as a single directly comparable numerical scale.

### 13.5 Footprints reconstructed from FIRMS scan/track are approximations

The FIRMS CSV `scan` and `track` values do not provide the actual Level-2 pixel-corner coordinates or orbit orientation. The current footprint provider creates an axis-aligned rectangle in a local Lambert Azimuthal Equal-Area (LAEA) projection as a proxy. It is tagged `FIRMS_APPROX_SCAN_TRACK`; it is not a true sensor pixel polygon. Where dimensions are unavailable, the implementation can use an explicitly tagged point-centroid fallback.

Area, intersection, overlap and boundary calculations are performed in projected metric geometry rather than directly in longitude/latitude degrees. Exact Level-2 corner geolocation should replace the proxy when a verified Level-2 provider is implemented.

### 13.6 Matching is many-to-many

All candidate pairs that meet the configured rules are retained. The module does not force one MODIS observation to match exactly one VIIRS observation, and it does not use the matching output as a definitive physical-fire identity. Graph degrees and connected components distinguish single candidates, multiple candidates, ambiguous groups and unmatched observations.

The default 2 km / 30 minute baseline limits and the footprint boundary limit are configurable exploratory parameters. They require sensitivity analysis and regional/product-specific validation before being presented as scientifically optimal thresholds.

### 13.7 FRP values are not yet fully harmonized

- The source FRP value is retained as the raw value.
- The adjusted FRP field remains NULL unless a separate implemented FRP observation operator is supplied.
- Current detection-limit calculations are preliminary diagnostics based on reference values and approximate pixel-area scaling.
- These diagnostics are not a complete MODIS–VIIRS FRP observation operator and are not universal thresholds.
- MODIS and VIIRS FRP must remain sensor-specific; do not add them into a single cross-sensor total.
- FRP is not Fire Radiative Energy (FRE).

Do not describe the current FRP output as a validated harmonized FRP product.

### 13.8 Common-grid values are allocated representations

The grid uses an equal-area projection and footprint-intersection allocation. A pixel's contribution to a cell is weighted by:

```text
weight = Area(pixel footprint ∩ grid cell) / Area(pixel footprint)
```

Therefore:

- `modis_count` and `viirs_count` are fractional pixel-equivalent counts, not necessarily integer event counts.
- Cell FRP values are footprint-allocated representations of original observation FRP, not new independent measurements made in each cell.
- Sensor-specific FRP remains separate.
- `matched_group_count` reflects candidate-graph components represented in a cell, not confirmed physical fires.
- For FIRMS-only inputs, `valid_area_fraction` remains NULL because valid observation opportunity is not known.
- Grid cells without detections are not written as empty zero-activity rows; they are implicitly `UNKNOWN`.

### 13.9 Sparse grid storage

The grid builder stores only cell/time combinations touched by eligible observation footprints. It does not materialize every possible grid cell for every time period. This avoids very large empty grids for broad bounding boxes and long date ranges. Omitted cells are not zero-fire cells; they are unknown under the FIRMS-only evidence model.

---

## 14. Troubleshooting

### `ModuleNotFoundError: No module named 'sanyam_firms_portable'`

The project currently uses a flat module layout. Search for old package-style imports:

```bash
grep -RIn "sanyam_firms_portable" --include="*.py" .
```

Change imports such as:

```python
from sanyam_firms_portable.db import DatabaseRepository, connect_db
```

to:

```python
from db import DatabaseRepository, connect_db
```

To update Python files that still contain that exact import prefix from the repository root:

```bash
grep -Rl "from sanyam_firms_portable\.db import" --include="*.py" . | \
  xargs -r sed -i 's/from sanyam_firms_portable\.db import/from db import/g'
```

Then verify:

```bash
python -c "import db, firms_ingest, qc_and_opportunity, matching_and_measurement; print('ALL MODULE IMPORTS SUCCESSFUL')"
```

Do not rename the project directory or create a nested package just to resolve this flat-layout import error.

### `FIRMS_MAP_KEY is missing`

1. Confirm `.env` exists in the repository root.
2. Confirm it contains `FIRMS_MAP_KEY=...` with a valid key.
3. Confirm `python-dotenv` is installed.
4. Alternatively, export the key in the current shell:

```bash
export FIRMS_MAP_KEY="YOUR_ACTUAL_FIRMS_MAP_KEY"
```

Do not paste the key into a public terminal screenshot or issue.

### FIRMS returns HTTP 429 or a transaction/quota warning

- Run `python firms_ingest.py status` to inspect current MAP_KEY usage.
- FIRMS transaction usage is reported over a rolling time window; requests made by other applications using the same key can also count.
- Let the limit window reset before retrying.
- Re-run the same ingestion request with `--resume` so exact completed chunks can be skipped.
- Keep `--chunk-days 1` for a conservative initial run and avoid repeatedly requesting broad date ranges.

### No source/date windows are available

Run:

```bash
python firms_ingest.py availability
```

Check that the requested dates overlap the relevant dataset's advertised availability. Confirm the source/family selection and the requested date range.

### Tests fail after changing a module

Run the failed module's test file first, then the full suite:

```bash
python -m unittest discover -s tests -p "test_matching_and_measurement.py" -v
python -m unittest discover -s tests -v
```

Do not assume a previous test result applies to modified files.

### Grid aggregation completes but candidate-link export is empty

Confirm that the grid command used `--matching-run-id` with the intended completed matching run. Without that association, the aggregation run does not know which separate matching run's candidate links should be included in its export.

### Export says the processing run is not completed or cannot be found

List aggregation runs:

```bash
python matching_and_measurement.py runs --module AGGREGATION --limit 20
```

Use the `processing_run_id` from a `COMPLETED` aggregation run. Do not pass the `--matching-run-id` value as the export ID.

### PostgreSQL connection fails

Check that the PostgreSQL server is running and reachable, the database exists, the role has required privileges, and the URL uses the `postgresql+psycopg://` driver. PostgreSQL DDL compilation in unit tests does not replace a real connection test.

### Grid is slow or the process is terminated

The current implementation uses sparse cell creation and bounded inserts. For debugging or a first smoke test, reduce the date range or bounding box, then increase the region and time span incrementally. Avoid treating sparse/omitted cells as zero detections.

---

## 15. Recommended research workflow

For a reproducible scientific run, use this order:

1. Record the study bbox, date range, datasets, collections, processing type and API availability.
2. Ingest SP data first; explicitly document any NRT data used.
3. Retain the original CSV snapshots and their checksums.
4. Run QC and record the QC processing-run ID and method version.
5. Run opportunity evaluation and preserve `UNKNOWN` where FIRMS evidence is incomplete.
6. Run baseline and footprint-aware matching as separate methods; retain all candidate edges.
7. Perform threshold sensitivity experiments instead of treating the default distances/times as universal.
8. Run FRP diagnostics while preserving raw values; do not claim full FRP harmonization at this stage.
9. Build the common grid and explicitly link it to the matching run with `--matching-run-id`.
10. Export GeoJSON, archive the run configuration and report the known limitations.
11. When Level-2 products become available, implement and validate a separate provider for true geolocation, QA masks and observation opportunity before upgrading the scientific claims.

The guiding principle is to preserve the original observation, provenance, uncertainty and limitations rather than silently converting incomplete satellite evidence into a definitive fire-event record.

---

## Development status

This is a research software milestone intended for iterative validation. A successful ingestion or processing command confirms that the configured software workflow executed; it does not, by itself, demonstrate scientific validity or guarantee that the selected parameters are suitable for every sensor, collection, region or date range.
