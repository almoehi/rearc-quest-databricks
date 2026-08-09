"""Integration tests against live Unity Catalog tables.

Runs SQL queries via the serverless SQL warehouse using the configured
Databricks CLI profile. Skipped automatically when the warehouse is
unreachable (no credentials, network, etc.).

Run with:
    uv run --group dev pytest tests/test_integration.py -v
Override defaults:
    DATABRICKS_CONFIG_PROFILE=staging INTEGRATION_CATALOG=rearc_staging \\
        uv run --group dev pytest tests/test_integration.py -v
"""

import os

import pytest

CATALOG = os.environ.get("INTEGRATION_CATALOG", "rearc_dev")
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "dev")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "47d991b585f89155")
SCHEMA_BRONZE = f"{CATALOG}.bronze"
SCHEMA_SILVER = f"{CATALOG}.silver"
SCHEMA_GOLD = f"{CATALOG}.gold"


@pytest.fixture(scope="module")
def db():
    """SQL cursor against the serverless warehouse via the configured CLI profile, or skip."""
    try:
        from databricks import sql
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(profile=PROFILE)
        host = w.config.host.rstrip("/").replace("https://", "")
        http_path = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=w.config.token,
        )
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()
    except Exception as e:
        pytest.skip(f"SQL warehouse unavailable: {e}")


def _scalar(cursor, query: str):
    cursor.execute(query)
    return cursor.fetchone()[0]


# ── Row count sanity checks ───────────────────────────────────────────────────

def test_silver_data_row_count_reasonable(db):
    """silver.data should contain at least 95% of bronze.raw_data rows."""
    bronze_count = _scalar(db, f"SELECT COUNT(*) FROM {SCHEMA_BRONZE}.raw_data")
    silver_count = _scalar(db, f"SELECT COUNT(*) FROM {SCHEMA_SILVER}.data")
    assert silver_count >= bronze_count * 0.95, (
        f"silver.data ({silver_count:,}) dropped more than 5% of bronze.raw_data ({bronze_count:,})"
    )


def test_gold_tbl_bls_productivity_all_series_not_larger_than_silver_data(db):
    """gold.tbl_bls_productivity_all_series can only shrink relative to silver.data (quality gates drop rows, never add them)."""
    silver_count = _scalar(db, f"SELECT COUNT(*) FROM {SCHEMA_SILVER}.data")
    wide_count   = _scalar(db, f"SELECT COUNT(*) FROM {SCHEMA_GOLD}.tbl_bls_productivity_all_series")
    assert wide_count <= silver_count, (
        f"gold.tbl_bls_productivity_all_series ({wide_count:,}) is larger than silver.data ({silver_count:,})"
    )


# ── Quality gate enforcement ──────────────────────────────────────────────────

def test_gold_tbl_bls_productivity_all_series_no_null_keys(db):
    """gold.tbl_bls_productivity_all_series must have zero rows with NULL in any primary key or value column."""
    null_count = _scalar(
        db,
        f"""
        SELECT COUNT(*) FROM {SCHEMA_GOLD}.tbl_bls_productivity_all_series
        WHERE series_id IS NULL OR year IS NULL OR value IS NULL OR period IS NULL
        """,
    )
    assert null_count == 0, f"gold.tbl_bls_productivity_all_series contains {null_count:,} rows with NULL key/value columns"


# ── Spot-check: known series ──────────────────────────────────────────────────

def test_gold_tbl_bls_productivity_all_series_prs30006032_q01_populated(db):
    """PRS30006032 Q01 rows should exist in gold.tbl_bls_productivity_all_series with all dimension columns non-null."""
    db.execute(
        f"""
        SELECT year, sector_name, measure_text, duration_text, seasonal, series_label
        FROM {SCHEMA_GOLD}.tbl_bls_productivity_all_series
        WHERE series_id = 'PRS30006032' AND period = 'Q01'
        ORDER BY year
        """
    )
    rows = db.fetchall()
    assert len(rows) >= 1, "No Q01 rows found for series PRS30006032 in gold.tbl_bls_productivity_all_series"
    for row in rows:
        assert row["sector_name"]   is not None, f"sector_name is NULL for year {row['year']}"
        assert row["measure_text"]  is not None, f"measure_text is NULL for year {row['year']}"
        assert row["duration_text"] is not None, f"duration_text is NULL for year {row['year']}"
        assert row["seasonal"]      is not None, f"seasonal is NULL for year {row['year']}"
        assert row["series_label"]  is not None, f"series_label is NULL for year {row['year']}"


# ── Analytical question smoke checks ─────────────────────────────────────────

def test_population_stats_2013_2018_returns_one_row(db):
    db.execute(f"SELECT * FROM {SCHEMA_GOLD}.v_population_stats_2013_2018")
    rows = db.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["year_from"] == 2013
    assert row["year_to"] == 2018
    assert row["year_count"] == 6
    assert row["mean_population"] > 0
    assert row["stddev_population"] > 0


def test_series_best_year_one_row_per_series(db):
    total    = _scalar(db, f"SELECT COUNT(*) FROM {SCHEMA_GOLD}.tbl_series_best_year")
    distinct = _scalar(db, f"SELECT COUNT(DISTINCT series_id) FROM {SCHEMA_GOLD}.tbl_series_best_year")
    assert total == distinct, "tbl_series_best_year has multiple rows for some series_id"


def test_series_q01_population_prs30006032(db):
    db.execute(
        f"""
        SELECT year, us_population
        FROM {SCHEMA_GOLD}.v_series_q01_population
        WHERE series_id = 'PRS30006032'
        ORDER BY year
        """
    )
    rows = db.fetchall()
    assert len(rows) >= 1, "No rows found for PRS30006032 in v_series_q01_population"
    by_year = {row["year"]: row for row in rows}
    for yr in [2013, 2014, 2015, 2016, 2017, 2018]:
        if yr in by_year:
            assert by_year[yr]["us_population"] is not None, f"Expected population for year {yr}"
    if 2020 in by_year:
        assert by_year[2020]["us_population"] is None, "Expected NULL population for 2020"
