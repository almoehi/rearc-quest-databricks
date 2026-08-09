import dlt
from pyspark.sql import DataFrame

_bronze = "bronze"
_silver = "silver"

from silver_transforms import (
    clean_strings,
    deduplicate,
    schema_data,
    schema_population,
    schema_series,
    to_upper,
)


def _annotate(df: DataFrame, comments: dict[str, str]) -> DataFrame:
    """Attach column-level comments via DataFrame metadata so they are persisted to Delta/UC."""
    for col_name, comment in comments.items():
        df = df.withMetadata(col_name, {"comment": comment})
    return df


# ── Fact table ────────────────────────────────────────────────────────────────

@dlt.table(
    name=f"{_silver}.data",
    comment="BLS productivity time series: typed, string-cleaned, and deduplicated from bronze.raw_data; one row per (series_id, year, period); revised estimates (footnote_codes='R') supersede originals for the same composite key.",
    table_properties={"quality": "silver"},
    cluster_by=["series_id", "year", "period_type"],
)
@dlt.expect_all_or_drop({
    "valid_series_id":    "series_id IS NOT NULL",
    "valid_year_range":   "year > 1900",
    "valid_value":        "value IS NOT NULL",
    "valid_period_format": "period RLIKE '^(Q0[1-5]|A01)$'",
})
def silver_data():
    raw = dlt.read(f"{_bronze}.raw_data")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "series_id", "period", "footnote_codes")
    typed = schema_data(uppercased)
    result = deduplicate(typed, ["series_id", "year", "period"], tiebreak_col="footnote_codes", prefer_value="R")
    return _annotate(result, {
        "series_id": "Unique BLS series identifier encoding sector, measure, duration, and seasonal adjustment (e.g., PRS30006032); composite primary key component.",
        "year": "Calendar year of the observation, cast from the raw BLS string field to integer.",
        "period": "BLS period code for the sub-annual interval (e.g., Q01 = first quarter, Q05 = five-quarter moving average, A01 = annual average); composite primary key component.",
        "value": "Measured statistic for this series/year/period in units specific to the measure (index points or percent change); NULL when the source string could not be parsed as a number.",
        "footnote_codes": "BLS footnote indicator, normalised to upper case; 'R' denotes a revised estimate that supersedes a prior release for the same series/year/period key.",
        "period_start_date": "First calendar date of the period derived from year and period code (Q01→Jan 1, Q02→Apr 1, Q03→Jul 1, Q04→Oct 1, A01→Jan 1); NULL for Q05 (moving average) and unrecognised codes.",
        "period_type": "Derived classification of the BLS period code: 'quarterly' for Q-prefixed codes (Q01–Q05), 'annual' for A-prefixed codes (A01), 'other' for any remaining codes.",
        "period_num": "Numeric sequence within the period type extracted from the period code suffix (e.g., Q01→1, Q04→4, A01→1); used to sort periods chronologically within a year.",
    })


# ── Series metadata ───────────────────────────────────────────────────────────

@dlt.table(
    name=f"{_silver}.series",
    comment="BLS pr.series metadata: typed, string-cleaned, and deduplicated from bronze.raw_series; one row per series_id mapping each time series to its economic attributes.",
    table_properties={"quality": "silver"},
    cluster_by=["series_id"],
)
@dlt.expect_all_or_drop({
    "valid_series_id": "series_id IS NOT NULL",
    "valid_begin_year": "begin_year > 1900",
    "valid_end_year":   "end_year > 1900",
})
def silver_series():
    raw = dlt.read(f"{_bronze}.raw_series")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "sector_code", "class_code", "measure_code", "duration_code", "seasonal", "begin_period", "end_period", "footnote_codes")
    typed = schema_series(uppercased)
    result = deduplicate(typed, ["series_id"])
    return _annotate(result, {
        "series_id": "Unique BLS series identifier; primary key for this table.",
        "sector_code": "BLS economic sector code (normalised to upper case) linking to silver.lookup_sector.",
        "class_code": "BLS class sub-code (normalised to upper case) linking to silver.lookup_class.",
        "measure_code": "BLS measure code (normalised to upper case) linking to silver.lookup_measure.",
        "duration_code": "BLS duration code (normalised to upper case) linking to silver.lookup_duration.",
        "seasonal": "Seasonal adjustment flag, normalised to upper case: 'S' = seasonally adjusted, 'U' = not seasonally adjusted.",
        "base_year": "Reference year for index-type measures, cast to integer; NULL for non-index measures.",
        "footnote_codes": "Series-level footnote indicator, normalised to upper case.",
        "begin_year": "First year for which this series has data, cast to integer.",
        "begin_period": "First period code for which this series has data, normalised to upper case.",
        "end_year": "Last year for which this series has data, cast to integer.",
        "end_period": "Last period code for which this series has data, normalised to upper case.",
    })


# ── Lookup tables ─────────────────────────────────────────────────────────────

@dlt.table(
    name=f"{_silver}.lookup_measure",
    comment="BLS pr.measure reference table: typed, string-cleaned, and deduplicated from bronze.raw_measure; one row per measure_code.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_measure_code", "measure_code IS NOT NULL")
def silver_lookup_measure():
    raw = dlt.read(f"{_bronze}.raw_measure")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "measure_code", "selectable")
    result = deduplicate(uppercased, ["measure_code"])
    return _annotate(result, {
        "measure_code": "BLS measure code, normalised to upper case (e.g., 01 = output per hour, 11 = unit labor costs); primary key.",
        "measure_text": "Human-readable description of the economic measure.",
        "display_level": "BLS UI metadata: hierarchical display level.",
        "selectable": "BLS UI metadata: whether the code is user-selectable ('T'/'F'), normalised to upper case.",
        "sort_sequence": "BLS UI metadata: display sort order.",
    })


@dlt.table(
    name=f"{_silver}.lookup_sector",
    comment="BLS pr.sector reference table: typed, string-cleaned, and deduplicated from bronze.raw_sector; one row per sector_code.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_sector_code", "sector_code IS NOT NULL")
def silver_lookup_sector():
    raw = dlt.read(f"{_bronze}.raw_sector")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "sector_code", "selectable")
    result = deduplicate(uppercased, ["sector_code"])
    return _annotate(result, {
        "sector_code": "BLS economic sector code, normalised to upper case (e.g., 8500 = nonfarm business, 3000 = manufacturing); primary key.",
        "sector_name": "Human-readable name of the economic sector.",
        "display_level": "BLS UI metadata: hierarchical display level.",
        "selectable": "BLS UI metadata: whether the code is user-selectable ('T'/'F'), normalised to upper case.",
        "sort_sequence": "BLS UI metadata: display sort order.",
    })


@dlt.table(
    name=f"{_silver}.lookup_class",
    comment="BLS pr.class reference table: typed, string-cleaned, and deduplicated from bronze.raw_class; one row per class_code.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_class_code", "class_code IS NOT NULL")
def silver_lookup_class():
    raw = dlt.read(f"{_bronze}.raw_class")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "class_code", "selectable")
    result = deduplicate(uppercased, ["class_code"])
    return _annotate(result, {
        "class_code": "BLS class sub-code, normalised to upper case; primary key.",
        "class_text": "Human-readable description of the class sub-classification.",
        "display_level": "BLS UI metadata: hierarchical display level.",
        "selectable": "BLS UI metadata: whether the code is user-selectable ('T'/'F'), normalised to upper case.",
        "sort_sequence": "BLS UI metadata: display sort order.",
    })


@dlt.table(
    name=f"{_silver}.lookup_duration",
    comment="BLS pr.duration reference table: typed, string-cleaned, and deduplicated from bronze.raw_duration; one row per duration_code.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_duration_code", "duration_code IS NOT NULL")
def silver_lookup_duration():
    raw = dlt.read(f"{_bronze}.raw_duration")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "duration_code", "selectable")
    result = deduplicate(uppercased, ["duration_code"])
    return _annotate(result, {
        "duration_code": "BLS duration code, normalised to upper case (e.g., 1 = % change same quarter 1 year ago, 3 = % change from prior quarter, 6 = index); primary key.",
        "duration_text": "Human-readable description of the duration type.",
        "display_level": "BLS UI metadata: hierarchical display level.",
        "selectable": "BLS UI metadata: whether the code is user-selectable ('T'/'F'), normalised to upper case.",
        "sort_sequence": "BLS UI metadata: display sort order.",
    })


@dlt.table(
    name=f"{_silver}.lookup_footnote",
    comment="BLS pr.footnote reference table: typed, string-cleaned, and deduplicated from bronze.raw_footnote; one row per footnote_code; 'R' = revised estimate.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_footnote_code", "footnote_code IS NOT NULL")
def silver_lookup_footnote():
    raw = dlt.read(f"{_bronze}.raw_footnote")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "footnote_code")
    result = deduplicate(uppercased, ["footnote_code"])
    return _annotate(result, {
        "footnote_code": "BLS footnote indicator, normalised to upper case; 'R' denotes a revised estimate superseding a prior release; primary key.",
        "footnote_text": "Human-readable explanation of the footnote code.",
    })


@dlt.table(
    name=f"{_silver}.lookup_period",
    comment="BLS pr.period reference table: typed, string-cleaned, and deduplicated from bronze.raw_period; one row per period code (Q01–Q05, A01).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_period", "period IS NOT NULL")
def silver_lookup_period():
    raw = dlt.read(f"{_bronze}.raw_period")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "period")
    result = deduplicate(uppercased, ["period"])
    return _annotate(result, {
        "period": "BLS period code, normalised to upper case (e.g., Q01 = first quarter, Q05 = five-quarter moving average, A01 = annual average); primary key.",
        "period_abbr": "Short abbreviation for the period as used in BLS publications (e.g., '1st Qtr').",
        "period_name": "Full human-readable name of the period (e.g., '1st Quarter').",
    })


@dlt.table(
    name=f"{_silver}.lookup_seasonal",
    comment="BLS pr.seasonal reference table: typed, string-cleaned, column names normalised to snake_case, and deduplicated from bronze.raw_seasonal; 'S' = seasonally adjusted, 'U' = not seasonally adjusted.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_seasonal_code", "seasonal_code IS NOT NULL")
def silver_lookup_seasonal():
    raw = dlt.read(f"{_bronze}.raw_seasonal")
    # bronze.raw_seasonal uses CamelCase column names (Seasonal_code, Seasonal_text) — rename first
    renamed = (
        raw.withColumnRenamed("Seasonal_code", "seasonal_code")
           .withColumnRenamed("Seasonal_text", "seasonal_text")
    )
    cleaned = clean_strings(renamed)
    uppercased = to_upper(cleaned, "seasonal_code")
    result = deduplicate(uppercased, ["seasonal_code"])
    return _annotate(result, {
        "seasonal_code": "BLS seasonal adjustment code, normalised to upper case: 'S' = seasonally adjusted, 'U' = not seasonally adjusted; primary key.",
        "seasonal_text": "Human-readable description of the seasonal adjustment code.",
    })


@dlt.table(
    name=f"{_silver}.lookup_population",
    comment="US annual total population estimates from the DataUSA ACS Tesseract API: typed, string-cleaned, and deduplicated from bronze.raw_population; one row per (nation_id, year); 2020 is absent due to the COVID-19 census gap.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({
    "valid_year_range":  "year > 1900",
    "valid_population":  "population IS NOT NULL AND population > 0",
})
def silver_lookup_population():
    raw = dlt.read(f"{_bronze}.raw_population")
    cleaned = clean_strings(raw)
    uppercased = to_upper(cleaned, "nation_id")
    typed = schema_population(uppercased)
    result = deduplicate(typed, ["nation_id", "year"])
    return _annotate(result, {
        "nation_id": "ACS geographic entity identifier in FIPS format (e.g., '01000US' for the United States), normalised to upper case; composite primary key component.",
        "nation": "Human-readable name of the geographic entity (e.g., 'United States').",
        "year": "Calendar year of the ACS population estimate, cast from string to integer; 2020 is absent because the Census Bureau did not publish a standard ACS 1-year estimate that year due to COVID-19 data quality concerns; composite primary key component.",
        "population": "Estimated total population of the nation for the given year, cast to a whole-number long integer.",
    })
