import pytest
from pyspark.sql import Row
from silver_transforms import (
    clean_strings,
    deduplicate,
    schema_data,
    schema_population,
    schema_series,
    to_upper,
)


# ── clean_strings ─────────────────────────────────────────────────────────────

def test_clean_strings_trims_whitespace(spark):
    df = spark.createDataFrame([Row(code="  ABC  ", label="  foo  ")])
    result = clean_strings(df).collect()[0]
    assert result["code"] == "ABC"
    assert result["label"] == "foo"


def test_clean_strings_empty_to_null(spark):
    df = spark.createDataFrame([Row(code="", label="ok")])
    result = clean_strings(df).collect()[0]
    assert result["code"] is None
    assert result["label"] == "ok"


def test_clean_strings_whitespace_only_to_null(spark):
    df = spark.createDataFrame([Row(code="   ", label="x")])
    result = clean_strings(df).collect()[0]
    assert result["code"] is None


def test_clean_strings_preserves_null(spark):
    df = spark.createDataFrame([(None, "x")], schema="code string, label string")
    result = clean_strings(df).collect()[0]
    assert result["code"] is None


# ── to_upper ──────────────────────────────────────────────────────────────────

def test_to_upper_uppercases(spark):
    df = spark.createDataFrame([Row(measure_code="01a", seasonal="s")])
    result = to_upper(df, "measure_code", "seasonal").collect()[0]
    assert result["measure_code"] == "01A"
    assert result["seasonal"] == "S"


def test_to_upper_preserves_other_cols(spark):
    df = spark.createDataFrame([Row(code="ab", label="keep me")])
    result = to_upper(df, "code").collect()[0]
    assert result["code"] == "AB"
    assert result["label"] == "keep me"


# ── deduplicate ───────────────────────────────────────────────────────────────

def test_deduplicate_removes_exact_dupes(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", year=1995, period="Q01", value=2.6, footnote_codes=""),
        Row(series_id="PRS001", year=1995, period="Q01", value=2.6, footnote_codes=""),
        Row(series_id="PRS001", year=1995, period="Q02", value=1.0, footnote_codes=""),
    ])
    assert deduplicate(df, ["series_id", "year", "period"]).count() == 2


def test_deduplicate_revised_wins(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", year=1995, period="Q01", value=1.0, footnote_codes=""),
        Row(series_id="PRS001", year=1995, period="Q01", value=1.5, footnote_codes="R"),
    ])
    result = deduplicate(df, ["series_id", "year", "period"], tiebreak_col="footnote_codes", prefer_value="R").collect()
    assert len(result) == 1
    assert result[0]["value"] == 1.5
    assert result[0]["footnote_codes"] == "R"


def test_deduplicate_single_row_unchanged(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", year=1995, period="Q01", value=1.0, footnote_codes=""),
        Row(series_id="PRS001", year=2000, period="Q01", value=1.1, footnote_codes=""),
    ])
    result = deduplicate(df, ["series_id", "year", "period"]).collect()
    assert len(result) == 2
    assert result[0]["value"] == 1.0


def test_deduplicate_simple_pk(spark):
    df = spark.createDataFrame([
        Row(measure_code="01", measure_text="First"),
        Row(measure_code="01", measure_text="Duplicate"),
        Row(measure_code="02", measure_text="Other"),
    ])
    assert deduplicate(df, ["measure_code"]).count() == 2


def test_deduplicate_half_paired_tiebreak_raises(spark):
    df = spark.createDataFrame([Row(id="a", flag="X")])
    with pytest.raises(ValueError, match="tiebreak_col and prefer_value"):
        deduplicate(df, ["id"], tiebreak_col="flag")


def test_deduplicate_half_paired_prefer_raises(spark):
    df = spark.createDataFrame([Row(id="a", flag="X")])
    with pytest.raises(ValueError, match="tiebreak_col and prefer_value"):
        deduplicate(df, ["id"], prefer_value="X")


# ── schema_data ───────────────────────────────────────────────────────────────

def test_schema_data_year_and_value(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", year="1995", period="Q01", value="2.6", footnote_codes=""),
        Row(series_id="PRS002", year="2000", period="A01", value="not-a-number", footnote_codes=""),
    ])
    result = schema_data(df)
    rows = {r["series_id"]: r for r in result.collect()}
    assert rows["PRS001"]["year"] == 1995
    assert abs(rows["PRS001"]["value"] - 2.6) < 0.001
    assert rows["PRS002"]["year"] == 2000
    assert rows["PRS002"]["value"] is None


def test_schema_data_period_type_quarterly(spark):
    df = spark.createDataFrame([Row(series_id="X", year="2020", period="Q01", value="1.0", footnote_codes="")])
    result = schema_data(df).collect()[0]
    assert result["period_type"] == "quarterly"
    assert result["period_num"] == 1


def test_schema_data_period_type_annual(spark):
    df = spark.createDataFrame([Row(series_id="X", year="2020", period="A01", value="1.0", footnote_codes="")])
    result = schema_data(df).collect()[0]
    assert result["period_type"] == "annual"
    assert result["period_num"] == 1


def test_schema_data_period_type_other(spark):
    df = spark.createDataFrame([Row(series_id="X", year="2020", period="X99", value="1.0", footnote_codes="")])
    result = schema_data(df).collect()[0]
    assert result["period_type"] == "other"


def test_schema_data_period_start_date_quarterly(spark):
    rows = [
        Row(series_id="A", year="2020", period="Q01", value="1.0", footnote_codes=""),
        Row(series_id="B", year="2020", period="Q02", value="1.0", footnote_codes=""),
        Row(series_id="C", year="2020", period="Q03", value="1.0", footnote_codes=""),
        Row(series_id="D", year="2020", period="Q04", value="1.0", footnote_codes=""),
    ]
    result = {r["series_id"]: r for r in schema_data(spark.createDataFrame(rows)).collect()}
    from datetime import date
    assert result["A"]["period_start_date"] == date(2020, 1, 1)
    assert result["B"]["period_start_date"] == date(2020, 4, 1)
    assert result["C"]["period_start_date"] == date(2020, 7, 1)
    assert result["D"]["period_start_date"] == date(2020, 10, 1)


def test_schema_data_period_start_date_annual(spark):
    df = spark.createDataFrame([Row(series_id="X", year="2020", period="A01", value="1.0", footnote_codes="")])
    from datetime import date
    result = schema_data(df).collect()[0]
    assert result["period_start_date"] == date(2020, 1, 1)


def test_schema_data_period_start_date_q05_is_null(spark):
    df = spark.createDataFrame([Row(series_id="X", year="2020", period="Q05", value="1.0", footnote_codes="")])
    result = schema_data(df).collect()[0]
    assert result["period_start_date"] is None


# ── schema_series ─────────────────────────────────────────────────────────────

def test_schema_series_year_fields(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", sector_code="8500", class_code="6",
            measure_code="01", duration_code="1", seasonal="S",
            base_year="2012", footnote_codes="",
            begin_year="1988", begin_period="Q01",
            end_year="2024", end_period="Q04"),
    ])
    result = schema_series(df).collect()[0]
    assert result["base_year"] == 2012
    assert result["begin_year"] == 1988
    assert result["end_year"] == 2024


def test_schema_series_unparseable_year_is_null(spark):
    df = spark.createDataFrame([
        Row(series_id="PRS001", sector_code="8500", class_code="6",
            measure_code="01", duration_code="1", seasonal="S",
            base_year="-", footnote_codes="",
            begin_year="1988", begin_period="Q01",
            end_year="2024", end_period="Q04"),
    ])
    result = schema_series(df).collect()[0]
    assert result["base_year"] is None


# ── schema_population ─────────────────────────────────────────────────────────

def test_schema_population_types(spark):
    df = spark.createDataFrame([Row(nation_id="01000US", nation="United States", year="2015", population="321418821.0")])
    result = schema_population(df).collect()[0]
    assert result["year"] == 2015
    assert result["population"] == 321418821
    assert isinstance(result["population"], int)


def test_schema_population_bad_year_is_null(spark):
    df = spark.createDataFrame([Row(nation_id="01000US", nation="United States", year="n/a", population="321418821.0")])
    result = schema_population(df).collect()[0]
    assert result["year"] is None


def test_schema_population_bad_population_is_null(spark):
    df = spark.createDataFrame([Row(nation_id="01000US", nation="United States", year="2015", population="N/A")])
    result = schema_population(df).collect()[0]
    assert result["population"] is None
