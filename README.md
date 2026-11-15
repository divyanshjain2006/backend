# SANYAM FIRMS ingestion — portable database refactor

This package refactors the FIRMS ingestion prototype to use SQLAlchemy Core for
persistence. It supports SQLite for local development and PostgreSQL for
production while keeping FIRMS fetching, normalization, quota protection,
source snapshots, SP/NRT selection, and resume logic in `firms_ingest.py`.

## Project layout

```text
sanyam_firms_portable/
├── db.py                       # SQLAlchemy Core schema + repository
├── firms_ingest.py             # FIRMS API, normalization, CLI
├── requirements.txt
├── .env.example
├── .gitignore
└── tests/
    └── test_firms_ingest.py
```

## Install and configure

```bash
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env                  # PowerShell: Copy-Item .env.example .env
```

Set the NASA FIRMS key in `.env`:

```env
FIRMS_MAP_KEY=YOUR_FIRMS_MAP_KEY
DATABASE_URL=sqlite:///data/sanyam_firms.sqlite3
LOG_LEVEL=INFO
```

Never commit `.env` or the local `data/` directory.

## SQLite development

The default is:

```text
sqlite:///data/sanyam_firms.sqlite3
```

It is used whenever `DATABASE_URL` is unset. SQLite-only connection PRAGMAs
(foreign keys, WAL, synchronous=NORMAL) are registered only for SQLite engines.

## PostgreSQL production

Install dependencies from `requirements.txt`; it includes Psycopg 3. Set:

```env
DATABASE_URL=postgresql+psycopg://username:password@hostname:5432/database_name
```

Use a URL-escaped password if it contains reserved URL characters. Keep the
connection string in deployment secrets rather than source control. The CLI
also accepts `--database-url` to override the environment variable.

Example:

```bash
python firms_ingest.py ingest \
  --start-date 2024-01-01 \
  --end-date 2024-01-05 \
  --bbox 73,26,78,30 \
  --mode sp-only
```

Legacy `--db path/to/file.sqlite3` remains as a SQLite-only shorthand; prefer
`DATABASE_URL` or `--database-url` for new deployments.

## Database design

`db.py` owns all persistence operations through `DatabaseRepository`:

- `has_completed_chunk`
- `create_run` using `INSERT ... RETURNING run_id`
- `finish_run`
- `insert_observations` using dialect-specific `ON CONFLICT DO NOTHING RETURNING`

Inserted and duplicate counts are derived from returned inserted source-row
hashes, not SQLite-only connection counters.

The core tables are:

- `ingestion_runs`: ingestion-batch/processing-run metadata.
- `raw_assets`: references and SHA-256 metadata for retained source CSV files.
- `observations`: decoded source-preserving observations.

Observation metadata includes explicit `sensor`, `collection`,
`footprint_source`, and `opportunity_state`. `acquired_at`, ingestion timestamps,
and run timestamps use the `UTCDateTime` SQLAlchemy type: it requires aware
UTC-compatible values, compiles to PostgreSQL `TIMESTAMP WITH TIME ZONE`, and
round-trips UTC-aware Python datetimes on SQLite.

Collection is parsed from the FIRMS `version` field (for example `6.1SP` ->
`6.1`, `2.0SP` -> `2`). Unknown version strings remain NULL rather than being
invented. The original `source_version` and all raw CSV fields remain retained.

`footprint_source='FIRMS_POINT_SCAN_TRACK'` identifies the current FIRMS
point/scan/track representation; it does not claim that a polygon footprint has
been reconstructed. `opportunity_state='UNKNOWN'` is a placeholder for FIRMS
hotspot rows. Under SKH Master Documentation §6.10, full area/time-specific
`ObservationOpportunity` records remain a separate future entity and should
not be inferred from a hotspot row.

The `raw_assets` table follows the SKH §6.10 separation of source-file metadata
from decoded observations. The existing `ingestion_runs.source_csv_path` and
`source_sha256` columns remain for compatibility with earlier SQLite data.

## Existing SQLite database migration

Opening an older SANYAM SQLite database with this version performs additive
schema migration for the four requested observation columns, backfills `sensor`
from known FIRMS dataset IDs and `collection` from retained version strings,
and creates `raw_assets`. Legacy timestamps are read as UTC-aware values by
the repository. Back up a production database before applying any migration.

## Tests

```bash
python -m unittest discover -s tests -v
```

The test suite exercises SQLite persistence, duplicate counting, resume logic,
additive migration from the previous SQLite schema, source-field normalization,
PostgreSQL DDL compilation (`IDENTITY`, `TIMESTAMP WITH TIME ZONE`) and
PostgreSQL upsert SQL compilation. A live PostgreSQL connection test requires
an actual test database and is not performed by the local SQLite test suite.

## Module 4 — Quality Control and Module 5 — Observation Opportunity

`qc_and_opportunity.py` implements the FIRMS-CSV versions of SKH Master
Documentation §§6.6 and 6.7. The SQLAlchemy Core schema and repository methods
for these modules live in `db.py`; both SQLite and PostgreSQL use the same
repository API.

New logical tables:

- `processing_runs`: module, method version, JSON configuration, timestamps,
  input/output counts and run status.
- `qc_results`: immutable per-observation QC results linked to a processing run.
- `observation_opportunities`: area/time/sensor/platform opportunity records,
  with an optional `observation_id` for traceable positive detections.

Run QC for observations not already processed by this QC method version:

```bash
python qc_and_opportunity.py qc
```

Re-run QC for all observations with a new immutable processing run:

```bash
python qc_and_opportunity.py qc --method-version 1.0.1 --reprocess
```

Evaluate FIRMS opportunity for a bounding box and inclusive UTC date range:

```bash
python qc_and_opportunity.py opportunity \
  --sensor VIIRS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

Query the latest stored opportunity summary for the same request:

```bash
python qc_and_opportunity.py summary \
  --sensor VIIRS \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

For a non-default database, put the global option before the command:

```bash
python qc_and_opportunity.py \
  --database-url "$DATABASE_URL" qc
```

QC never deletes source observations. It processes rows not yet evaluated by
the selected `method_version`; `--reprocess` deliberately creates a new set of
QC results instead of overwriting historical results. MODIS confidence is
interpreted as numeric 0–100; VIIRS categorical labels are retained and mapped
to LOW/NOMINAL/HIGH. Native `confidence_raw` is never modified. Missing or
nonpositive FRP disables FRP suitability but does not disable point/time
matching when coordinates and acquisition time are valid.

The FIRMS opportunity provider is deliberately conservative. FIRMS CSVs
contain positive hotspot rows but do not provide complete swath/cloud masks.
Consequently, a zero-row area/time query produces `UNKNOWN`, never
`VALID_NO_DETECTION`. An existing detection without a completed QC result is
also `UNKNOWN`; a QC-failed detection is retained as `INVALID_QUALITY`.

`OpportunityProvider` is the extension point for a future LAADS Level-2
adapter. A provider with swath, pixel-mask, geolocation and product-quality
evidence can write `VALID_NO_DETECTION`, `CLOUD_OBSCURED`, `NOT_COVERED` and
`INVALID_QUALITY` using `evidence_source='LEVEL2_SWATH_MASK'` without changing
the opportunity table or API record shape.

## Full test suite

```bash
python -m unittest discover -s tests -v
```

The suite covers FIRMS ingestion and Modules 4, 5, and 6; the current local
run passes 69 tests. PostgreSQL table DDL and dialect-specific schema properties
are compiled in tests; live PostgreSQL integration still requires a reachable
PostgreSQL test database.

## Module 6 — Matching, FRP diagnostics, and common-grid analysis

`matching_and_measurement.py` implements the FIRMS-CSV stage of SKH Master Documentation §§6.8, 6.10, and 7.2–7.7. It uses the portable `DatabaseRepository` in `db.py`, so local SQLite and PostgreSQL deployments share the same repository API.

Install the geospatial dependencies (included in `requirements.txt`):

```bash
pip install -r requirements.txt
```

Run Module 4 QC before matching. Matching reads only observations whose latest completed QC result has `suitability_for_matching = true`.

### Baseline Haversine/time matching

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  baseline \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --bbox 73,26,78,30 \
  --max-distance-km 2.0 \
  --max-time-minutes 30
```

The default exploratory limits are 2 km and 30 minutes. They are configurable research parameters, not universal validated constants. All qualifying pairs are retained; the module does not force one-to-one assignments. It also writes explicit `UNMATCHED` node rows and time-rejected spatial pairs.

### Footprint-aware matching

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  footprint \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --max-time-minutes 30 \
  --max-boundary-km 0.25
```

The module creates a local Lambert Azimuthal Equal-Area (LAEA) projection for the study bbox. Polygon areas, intersections, IoU, overlap ratios, and boundary distances are calculated in projected metres, never in longitude/latitude degrees.

For FIRMS rows with positive `scan` and `track`, it constructs an **axis-aligned approximate rectangle** in that local equal-area plane. The edge records are explicitly tagged `FIRMS_APPROX_SCAN_TRACK`. This is a proxy footprint, not a true Level-2 pixel-corner polygon: FIRMS CSV does not include orbit-heading/corner geolocation needed to reconstruct exact footprint orientation. If either observation lacks usable dimensions, a candidate may use the clearly labelled `POINT_CENTROID_FALLBACK`; its polygon overlap metrics remain NULL. `FootprintProvider` is injectable in the Python API; a future LAADS provider can return true pixel-corner polygons in the supplied local equal-area CRS and set `geometry_source="LEVEL2_PIXEL_CORNERS"`. Exports require the same provider source as the processing run to avoid silently substituting proxy geometry.

The bipartite graph stores node degree and connected-component identifiers. A connected component spanning multiple MODIS and VIIRS nodes is labelled `AMBIGUOUS_GROUP`; one-to-many or many-to-one components are labelled `MULTIPLE_CANDIDATES`; isolated eligible nodes are `UNMATCHED`. These statuses describe candidate structure, not physical fire identity.

### FRP measurement diagnostics

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  frp \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --bbox 73,26,78,30
```

`measurement_results.frp_raw_mw` copies the source FRP value. `frp_adjusted_mw` remains NULL unless a separately implemented `FRPObservationOperator` is supplied. The current detection-limit estimate uses the SKH §7.5.5 reference values (MODIS C6.1: 5.7 MW day / 3.7 MW night; VIIRS C2: 3.7 MW day / 0.7 MW night), scaled linearly by `scan * track` pixel-area ratio relative to nominal reference pixel area. This is an **initial diagnostic approximation only**, not the complete Sofiev & Kouznetsov pixel/grid observation operator and not a universal threshold. The operator interface is ready for a later versioned `fmidev/FRP_OOp` integration after its supported products and coefficients have been verified.

### Common-grid activity summary

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  grid \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --resolution-km 1.0 \
  --period-days 1 \
  --matching-run-id 12
```

Supported grid resolutions are 1, 2, and 5 km. The local equal-area grid is aligned within the selected study bbox. Pixel contributions use `Area(pixel ∩ cell) / Area(pixel)`. Consequently, `modis_count` and `viirs_count` are **fractional pixel-equivalent counts**, and FRP sums are footprint-allocated values, not independent measurements made in each cell. MODIS and VIIRS FRP totals remain separate; they are not added into a single cross-sensor total and they are not FRE. `matched_group_count` counts connected candidate-graph components represented in the cell, not confirmed physical fires.

For the FIRMS-only milestone, `valid_area_fraction` is NULL. A cell with no detections has `activity_status = UNKNOWN`, never an inferred inactive/no-fire state, because point-only FIRMS CSVs cannot establish swath coverage, cloud masking, or valid non-detection.

### QGIS / GEE GeoJSON export

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  export \
  --processing-run-id 12 \
  --output-dir data/exports/validation/run_12
```

The command writes and validates three RFC 7946 WGS84 GeoJSON FeatureCollections:

- `footprints.geojson` — approximate FIRMS scan/track polygons (or explicitly labelled point fallbacks), sensor, UTC acquisition time, raw FRP and QC status.
- `candidate_links.geojson` — LineStrings for non-rejected MODIS–VIIRS candidate associations and their metrics/status.
- `common_grid.geojson` — grid cells and activity/opportunity properties; `frp_sum` is an object with separate `MODIS_mw` and `VIIRS_mw` values, never an unsupported cross-sensor addition (empty for a processing run that has no aggregation results).

The Module 6 schema adds `candidate_associations`, `measurement_results`, and `aggregation_results`. `processing_runs.module_name` now supports `MATCHING`, `MEASUREMENT`, and `AGGREGATION`; existing SQLite databases are migrated while retaining prior processing-run records.


## Module 6 common-grid performance and run IDs

The common-grid builder uses a **sparse** storage strategy: it persists only
cell/period combinations touched by eligible detections. Grid cells with no
FIRMS point detections are not written as millions of empty database rows;
they are implicitly `UNKNOWN`, because FIRMS point-only CSV cannot prove a
valid non-detection. This is particularly important for large study areas and
daily grids. For example, the 73–78°E / 26–30°N box contains roughly 224,000
1-km grid slots before clipping its edge cells. Materializing every cell for
31 days would produce roughly 6.9 million rows even for an empty region.

The sparse implementation creates grid cells on demand around observation
footprints, clips them to the equal-area study polygon, calculates footprint
intersection weights, and inserts aggregation records in bounded batches.
Empty cell/period combinations remain implicit `UNKNOWN`; they are not
interpreted as no-fire or valid no-detection.

### Run the common grid

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  grid \
  --bbox 73,26,78,30 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --resolution-km 1.0 \
  --period-days 1 \
  --matching-run-id 12
```

The command prints a newly created `processing_run_id`. This ID is the
**AGGREGATION run ID** and is the one required by `export`. The value passed to
`--matching-run-id` is only an input reference to an earlier MATCHING run; it is
not the new grid run ID.

List the latest runs (including RUNNING/FAILED runs left by interrupted jobs):

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  runs --limit 20

python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  runs --module AGGREGATION --limit 20
```

Export only after the aggregation run reports `COMPLETED`:

```bash
python matching_and_measurement.py \
  --database-url "sqlite:///data/sanyam_firms.sqlite3" \
  export --processing-run-id <AGGREGATION_RUN_ID> \
  --output-dir data/exports/validation/run_<AGGREGATION_RUN_ID>
```
