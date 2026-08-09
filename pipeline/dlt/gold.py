import dlt
from pyspark.sql import DataFrame, functions as F

_silver = "silver"
_gold = "gold"

# PySpark (default — for DLT deployment):
from gold_transforms import annual_aggregation, best_year_per_series  # noqa: I001
from gold_transforms import build_wide_table
from gold_transforms import population_stats as _population_stats
from gold_transforms import quarterly_aggregation, series_q01_with_population
# SQL alternative — swap the import lines above for these:
# from gold_transforms_sql import annual_aggregation, best_year_per_series
# from gold_transforms_sql import population_stats as _population_stats
# from gold_transforms_sql import quarterly_aggregation, series_q01_with_population


def _annotate(df: DataFrame, comments: dict[str, str]) -> DataFrame:
    """Attach column-level comments via DataFrame metadata so they are persisted to Delta/UC."""
    for col_name, comment in comments.items():
        df = df.withMetadata(col_name, {"comment": comment})
    return df


# ── Wide enriched table (analytical base) ─────────────────────────────────────

@dlt.table(
    name=f"{_gold}.tbl_bls_productivity_all_series",
    comment="BLS productivity time series fully enriched with all dimension lookups from silver; one row per (series_id, year, period); series_label provides a human-readable description of each series; use this table as the base for all analytical queries.",
    table_properties={"quality": "gold"},
    cluster_by=["period_type", "year", "series_id"],
)
@dlt.expect_all_or_drop({
    "valid_series_id": "series_id IS NOT NULL",
    "valid_year":      "year > 1900",
    "valid_value":     "value IS NOT NULL",
    "valid_period":    "period IS NOT NULL",
})
def tbl_bls_productivity_all_series():
    return _annotate(
        build_wide_table(
            data=dlt.read(f"{_silver}.data"),
            series=dlt.read(f"{_silver}.series"),
            measure=dlt.read(f"{_silver}.lookup_measure"),
            sector=dlt.read(f"{_silver}.lookup_sector"),
            class_=dlt.read(f"{_silver}.lookup_class"),
            duration=dlt.read(f"{_silver}.lookup_duration"),
            seasonal=dlt.read(f"{_silver}.lookup_seasonal"),
            period=dlt.read(f"{_silver}.lookup_period"),
        ),
        {
            "series_id": "Unique BLS series identifier (e.g., PRS30006032); composite primary key component.",
            "year": "Calendar year of the observation.",
            "period": "BLS period code (e.g., Q01 = first quarter, A01 = annual average); composite primary key component.",
            "value": "Measured statistic in units specific to the measure (index points or percent change).",
            "footnote_codes": "BLS footnote indicator; 'R' = revised estimate.",
            "period_start_date": "First calendar date of the period (Q01→Jan 1, Q02→Apr 1, Q03→Jul 1, Q04→Oct 1, A01→Jan 1); NULL for Q05.",
            "period_type": "Derived period classification: 'quarterly' (Q01–Q05), 'annual' (A01), 'other'.",
            "period_num": "Numeric period sequence for sorting within a year.",
            "sector_code": "BLS economic sector code; NULL when series has no matching record in pr.series.",
            "class_code": "BLS class sub-code; NULL when series has no matching record in pr.series.",
            "measure_code": "BLS measure code; NULL when series has no matching record in pr.series.",
            "duration_code": "BLS duration code; NULL when series has no matching record in pr.series.",
            "seasonal": "Seasonal adjustment flag: 'S' = seasonally adjusted, 'U' = not seasonally adjusted.",
            "measure_text": "Human-readable measure description (e.g., 'Output per hour of all persons'); NULL when measure_code has no match.",
            "sector_name": "Human-readable sector name (e.g., 'Nonfarm Business'); NULL when sector_code has no match.",
            "class_text": "Human-readable class description; NULL when class_code has no match.",
            "duration_text": "Human-readable duration description (e.g., '% Change same quarter 1 year ago'); NULL when duration_code has no match.",
            "seasonal_text": "Human-readable seasonal adjustment description (e.g., 'Seasonally Adjusted').",
            "period_abbr": "Short period abbreviation (e.g., '1st Qtr').",
            "period_name": "Full period name (e.g., '1st Quarter').",
            "series_label": "Human-readable label combining sector, measure, duration, and seasonal adjustment; suitable for display without BLS code knowledge.",
        },
    )


# ── Fact tables (analytical aggregations over the wide table) ─────────────────

@dlt.table(
    name=f"{_gold}.tbl_productivity_quarterly",
    comment="BLS productivity statistics aggregated by sector, measure, year, and quarter across all series; each row represents the distribution of values for a given sector/measure/year/period combination.",
    table_properties={"quality": "gold"},
)
def tbl_productivity_quarterly():
    return _annotate(quarterly_aggregation(dlt.read(f"{_gold}.tbl_bls_productivity_all_series")), {
        "sector_code": "BLS economic sector code grouping the aggregated series.",
        "sector_name": "Human-readable name of the economic sector.",
        "measure_code": "BLS measure code grouping the aggregated series.",
        "measure_text": "Human-readable description of the economic measure.",
        "year": "Calendar year of the quarterly observations included in this aggregation group.",
        "period": "BLS quarter code (Q01 = first quarter through Q04 = fourth quarter).",
        "period_num": "Numeric quarter (1–4); used to order rows chronologically within a year.",
        "avg_value": "Arithmetic mean of the statistic across all series in this sector/measure/year/period group.",
        "min_value": "Minimum statistic value observed across the series in this group.",
        "max_value": "Maximum statistic value observed across the series in this group.",
        "series_count": "Number of distinct series contributing to this aggregation group.",
    })


@dlt.table(
    name=f"{_gold}.tbl_productivity_annual",
    comment="BLS productivity statistics aggregated by sector, measure, and year (annual rows only) across all series; each row represents the distribution of annual values for a given sector/measure/year combination.",
    table_properties={"quality": "gold"},
)
def tbl_productivity_annual():
    return _annotate(annual_aggregation(dlt.read(f"{_gold}.tbl_bls_productivity_all_series")), {
        "sector_code": "BLS economic sector code grouping the aggregated series.",
        "sector_name": "Human-readable name of the economic sector.",
        "measure_code": "BLS measure code grouping the aggregated series.",
        "measure_text": "Human-readable description of the economic measure.",
        "year": "Calendar year of the annual observations included in this aggregation group.",
        "avg_value": "Arithmetic mean of the annual statistic across all series in this group.",
        "min_value": "Minimum annual statistic value observed across the series in this group.",
        "max_value": "Maximum annual statistic value observed across the series in this group.",
        "series_count": "Number of distinct series contributing to this aggregation group.",
    })


@dlt.table(
    name=f"{_gold}.tbl_series_best_year",
    comment="Per-series best year: for each BLS series, the calendar year whose quarterly values sum to the highest total, enriched with a human-readable series label from gold.tbl_bls_productivity_all_series; one row per series_id.",
    table_properties={"quality": "gold"},
)
def tbl_series_best_year():
    return _annotate(best_year_per_series(dlt.read(f"{_gold}.tbl_bls_productivity_all_series")), {
        "series_id": "Unique BLS series identifier; primary key for this table.",
        "series_label": "Human-readable label combining sector name, measure description, duration type, and seasonal adjustment status.",
        "year": "Calendar year in which this series achieved its highest sum of quarterly values.",
        "annual_sum": "Sum of all quarterly (Q01–Q04) values for this series in the best year; the ranking criterion.",
    })


# ── Views (narrowed / specific slices of the wide and population tables) ──────

_POPULATION_STATS_COMMENTS = {
    "nation_id": "ACS geographic entity identifier in FIPS format (e.g., '01000US' for the United States).",
    "nation": "Human-readable name of the geographic entity (e.g., 'United States').",
    "mean_population": "Arithmetic mean of the annual population estimates across the years included in this view.",
    "stddev_population": "Population standard deviation of the annual population estimates within the view's year range.",
    "year_from": "Earliest ACS survey year included in the statistics (2013 for this view).",
    "year_to": "Latest ACS survey year included in the statistics (2018 for this view).",
    "year_count": "Number of ACS survey years included; 6 for the 2013–2018 window.",
}


@dlt.table(
    name=f"{_gold}.v_population_stats_2013_2018",
    comment="Population summary statistics (mean, stddev) narrowed to the 2013–2018 ACS window; answers the analytical question of average and variability of US population during that specific period.",
    table_properties={"quality": "gold"},
)
def v_population_stats_2013_2018():
    pop = dlt.read(f"{_silver}.lookup_population").where(F.col("year").between(2013, 2018))
    return _annotate(_population_stats(pop), _POPULATION_STATS_COMMENTS)


@dlt.table(
    name=f"{_gold}.v_series_q01_population",
    comment="First-quarter (Q01) values for every BLS series joined with US population for the same year; enables productivity-to-population ratio analysis; years without ACS population data (e.g., 2020) appear with NULL us_population.",
    table_properties={"quality": "gold"},
)
def v_series_q01_population():
    return _annotate(
        series_q01_with_population(
            wide=dlt.read(f"{_gold}.tbl_bls_productivity_all_series"),
            population=dlt.read(f"{_silver}.lookup_population"),
        ),
        {
            "series_id": "Unique BLS series identifier (e.g., PRS30006032).",
            "series_label": "Human-readable label combining sector name, measure description, duration type, and seasonal adjustment status.",
            "year": "Calendar year of the Q01 observation.",
            "period": "Always 'Q01' in this view.",
            "value": "Measured statistic value for this series in the first quarter of the given year.",
            "us_population": "Estimated US total population for the same calendar year from the ACS dataset; NULL for years not covered by ACS (notably 2020).",
        },
    )
