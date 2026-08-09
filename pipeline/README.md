# pipeline

Databricks Asset Bundle (DAB) that transforms raw BLS Productivity & Costs files (written by the crawler) into analytics-ready tables using Lakeflow Declarative Pipelines (formerly Delta Live Tables / DLT). Three medallion layers: bronze (raw ingest via Auto Loader), silver (typed, cleaned, deduplicated — one table per bronze source), gold (tbl_bls_productivity_all_series enriched table + analytical aggregations).

## Project layout

```
pipeline/
├── databricks.yml                         # DAB bundle: targets (dev / staging / prod), variables
├── setup.sh                               # bootstrap Unity Catalog (catalog, groups, grants)
├── teardown.sh                            # destroy all deployed resources including data
├── resources/
│   ├── pipeline.yml                       # DLT pipeline resource
│   ├── crawler_job.yml                    # Databricks Job that runs the crawler wheel
│   ├── volume.yml                         # Unity Catalog volume for raw TSV files
│   ├── permissions.yml                    # Unity Catalog schema grants (bronze / silver / gold)
│   └── economic_metrics.geniespace.yml    # Genie Space with sample questions and SQL hints
├── dlt/
│   ├── bronze.py                          # Auto Loader sources — one DLT table per file type
│   ├── silver.py                          # 10 tables (1:1 with bronze): typed, cleaned, deduped
│   ├── silver_transforms.py               # pure PySpark: clean_strings, to_upper, schema_*, deduplicate
│   ├── gold.py                            # gold.tbl_bls_productivity_all_series (enriched base) + analytical tables and views
│   ├── gold_transforms.py                 # pure PySpark: build_wide_table, aggregations, analytical queries
│   └── gold_transforms_sql.py             # SQL variants of the three analytical query functions
├── scripts/
│   └── test-in-docker.py                  # PEP 723 uv script: build image and run pytest in Docker
└── tests/
    ├── conftest.py                        # local Spark session fixture (skips when Java absent)
    ├── test_silver_transforms.py          # unit tests for silver_transforms.py
    ├── test_gold_transforms.py            # unit tests for gold_transforms.py + gold_transforms_sql.py
    └── test_integration.py               # live tests via SQL warehouse (requires Databricks credentials)
```

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `pyspark` | ≥ 3.5 | local unit tests — Databricks Runtime provides it at pipeline runtime |
| `pytest` | ≥ 8.0 | test runner (dev) |
| `databricks-connect` | ≥ 16.1.7 | Databricks Connect SDK (transitive dep, kept for SDK utilities) |
| `databricks-sql-connector` | ≥ 4.4.0 | SQL warehouse connector used by integration tests |

Requires Python ≥ 3.10. Java is only needed for local unit tests (see below).

## Prerequisites

1. **Databricks CLI** installed and authenticated with a named profile:
   ```bash
   brew install databricks
   databricks auth login --profile dev
   ```

2. **Unity Catalog** set up in your workspace with a catalog matching the target variable (default: `rearc_dev`):
   ```bash
   ./setup.sh        # bootstrap catalog, groups, and grants for dev
   ./teardown.sh     # destroy all deployed resources including data
   ```

3. **Crawler output** available at the path configured in `data_path` (dev default: `/Volumes/rearc_dev/bronze/bls_raw`). The DLT pipeline reads from:
   ```
   {data_path}/current/       # pr.data.*.Current
   {data_path}/backfill/      # pr.data.*.AllData
   {data_path}/series/        # pr.series
   {data_path}/enrichment/    # pr.class, pr.measure, … us_population
   ```

4. **Java** (optional — only needed for local unit tests; not required for Docker or integration tests):
   ```bash
   brew install openjdk
   ```

## Deploy (DAB)

```bash
cd pipeline

# Deploy to dev (authenticates with the 'dev' CLI profile)
databricks bundle deploy -p dev

# Deploy to a specific target
databricks bundle deploy -p dev --target staging
databricks bundle deploy -p dev --target prod

# Run the DLT pipeline after deploying
databricks bundle run bls_pipeline -p dev

# Run all bundle resources in order (crawler writes data, pipeline ingests it)
databricks bundle run bls_crawler -p dev && databricks bundle run bls_pipeline -p dev

# Destroy all deployed resources
databricks bundle destroy -p dev
```

Override variables at deploy time:
```bash
databricks bundle deploy -p dev --var catalog=my_catalog --var data_path=s3://my-bucket/bls-data
```

## Run tests

### Unit tests (local — requires Java)

Exercises the pure PySpark transform functions without Databricks (no DLT, no Unity Catalog, no network).

```bash
cd pipeline

# Run all unit tests
uv run --group dev pytest

# Verbose output
uv run --group dev pytest -v

# Filter to a specific module
uv run --group dev pytest tests/test_silver_transforms.py -v
```

If Java is not installed, the Spark session fixture skips all tests automatically.

### Unit tests (Docker — no local deps required)

Builds a container with Java + PySpark baked in and runs the full unit test suite inside it.

```bash
# Run all unit tests
uv run scripts/test-in-docker.py

# Pass pytest args
uv run scripts/test-in-docker.py -k schema

# Force Docker layer rebuild
uv run scripts/test-in-docker.py --no-cache
```

### Integration tests (live SQL warehouse)

Runs against live Unity Catalog tables via the serverless SQL warehouse. Requires Databricks credentials and a completed pipeline run.

```bash
uv run --group dev pytest tests/test_integration.py -v
```

Override defaults with environment variables:
```bash
DATABRICKS_CONFIG_PROFILE=staging \
INTEGRATION_CATALOG=staging_bls \
DATABRICKS_WAREHOUSE_ID=<warehouse-id> \
  uv run --group dev pytest tests/test_integration.py -v
```
