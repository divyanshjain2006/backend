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
