import gold_transforms
import gold_transforms_sql
import pytest
from gold_transforms import annual_aggregation, build_wide_table, quarterly_aggregation
from pyspark.sql import Row


@pytest.fixture(params=["pyspark", "sql"], ids=["pyspark", "sql"])
def gt(request):
    """Parametrize the three analytical gold transforms across both implementations."""
    return gold_transforms if request.param == "pyspark" else gold_transforms_sql


# ── build_wide_table ──────────────────────────────────────────────────────────

def _data(spark):
    return spark.createDataFrame([
        Row(series_id="PRS001", year=2020, period="Q01", value=1.5, footnote_codes="",
            period_type="quarterly", period_num=1),
    ])


def _series(spark):
    return spark.createDataFrame([
        Row(series_id="PRS001", sector_code="8500", class_code="6",
            measure_code="01", duration_code="1", seasonal="S",
            base_year=2012, footnote_codes="",
            begin_year=1988, begin_period="Q01", end_year=2024, end_period="Q04"),
    ])


def _measure(spark):
    return spark.createDataFrame([Row(measure_code="01", measure_text="Output per hour", display_level="0", selectable="T", sort_sequence="1")])


def _sector(spark):
    return spark.createDataFrame([Row(sector_code="8500", sector_name="Nonfarm Business", display_level="0", selectable="T", sort_sequence="1")])


def _class(spark):
    return spark.createDataFrame([Row(class_code="6", class_text="Private", display_level="0", selectable="T", sort_sequence="1")])


def _duration(spark):
    return spark.createDataFrame([Row(duration_code="1", duration_text="% Change same quarter 1 year ago", display_level="0", selectable="T", sort_sequence="1")])


def _seasonal(spark):
    return spark.createDataFrame([Row(seasonal_code="S", seasonal_text="Seasonally Adjusted")])


def _period_lookup(spark):
    return spark.createDataFrame([Row(period="Q01", period_abbr="1st Qtr", period_name="1st Quarter")])


def _wide(spark):
    return build_wide_table(
        data=_data(spark),
        series=_series(spark),
        measure=_measure(spark),
        sector=_sector(spark),
        class_=_class(spark),
        duration=_duration(spark),
        seasonal=_seasonal(spark),
        period=_period_lookup(spark),
    )


def test_wide_table_joins_dimensions(spark):
    result = _wide(spark).collect()[0]
    assert result["sector_name"] == "Nonfarm Business"
    assert result["measure_text"] == "Output per hour"
    assert result["duration_text"] == "% Change same quarter 1 year ago"
    assert result["seasonal_text"] == "Seasonally Adjusted"
    assert result["period_abbr"] == "1st Qtr"
    assert result["class_text"] == "Private"


def test_wide_table_series_label(spark):
    result = _wide(spark).collect()[0]
    label = result["series_label"]
    assert "Nonfarm Business" in label
    assert "Output per hour" in label
    assert "% Change same quarter 1 year ago" in label
    assert "Seasonally Adjusted" in label


def test_wide_table_unmatched_series_kept(spark):
    data = spark.createDataFrame([
        Row(series_id="UNKNOWN", year=2020, period="Q01", value=1.0, footnote_codes="",
            period_type="quarterly", period_num=1),
    ])
    result = build_wide_table(
        data=data,
        series=_series(spark),
        measure=_measure(spark),
        sector=_sector(spark),
        class_=_class(spark),
        duration=_duration(spark),
        seasonal=_seasonal(spark),
        period=_period_lookup(spark),
    )
    assert result.count() == 1
    row = result.collect()[0]
    assert row["sector_name"] is None
    assert row["measure_text"] is None


def test_wide_table_not_seasonally_adjusted_label(spark):
    data_u = spark.createDataFrame([
        Row(series_id="PRS002", year=2020, period="Q01", value=1.0, footnote_codes="",
            period_type="quarterly", period_num=1),
    ])
    series_u = spark.createDataFrame([
        Row(series_id="PRS002", sector_code="8500", class_code="6",
            measure_code="01", duration_code="1", seasonal="U",
            base_year=2012, footnote_codes="",
            begin_year=1988, begin_period="Q01", end_year=2024, end_period="Q04"),
    ])
    seasonal_both = spark.createDataFrame([
        Row(seasonal_code="S", seasonal_text="Seasonally Adjusted"),
        Row(seasonal_code="U", seasonal_text="Not Seasonally Adjusted"),
    ])
    result = build_wide_table(
        data=data_u, series=series_u,
        measure=_measure(spark), sector=_sector(spark), class_=_class(spark),
        duration=_duration(spark), seasonal=seasonal_both, period=_period_lookup(spark),
    )
    assert "Not Seasonally Adjusted" in result.collect()[0]["series_label"]


# ── quarterly / annual aggregation (PySpark only — SQL re-exports identical) ──

def _enriched(spark):
    return spark.createDataFrame([
        Row(sector_code="8500", sector_name="Nonfarm Business",
            measure_code="01", measure_text="Employment", duration_code="1",
            seasonal="S", year=1995, period="Q01", period_type="quarterly", period_num=1, value=2.0,
            series_label="Nonfarm Business — Employment, % Change (Seasonally Adjusted)"),
        Row(sector_code="8500", sector_name="Nonfarm Business",
            measure_code="01", measure_text="Employment", duration_code="1",
            seasonal="S", year=1995, period="Q01", period_type="quarterly", period_num=1, value=4.0,
            series_label="Nonfarm Business — Employment, % Change (Seasonally Adjusted)"),
        Row(sector_code="8500", sector_name="Nonfarm Business",
            measure_code="01", measure_text="Employment", duration_code="1",
            seasonal="S", year=1995, period="Q02", period_type="quarterly", period_num=2, value=3.0,
            series_label="Nonfarm Business — Employment, % Change (Seasonally Adjusted)"),
        Row(sector_code="8500", sector_name="Nonfarm Business",
            measure_code="01", measure_text="Employment", duration_code="1",
            seasonal="S", year=1995, period="A01", period_type="annual", period_num=1, value=5.0,
            series_label="Nonfarm Business — Employment, % Change (Seasonally Adjusted)"),
    ])


def test_quarterly_agg_groups(spark):
    result = quarterly_aggregation(_enriched(spark))
    assert result.count() == 2  # Q01 and Q02


def test_quarterly_avg(spark):
    result = quarterly_aggregation(_enriched(spark))
    q1 = next(r for r in result.collect() if r["period"] == "Q01")
    assert abs(q1["avg_value"] - 3.0) < 0.001
    assert q1["series_count"] == 2


def test_quarterly_excludes_annual(spark):
    result = quarterly_aggregation(_enriched(spark))
    assert all(r["period"] != "A01" for r in result.collect())


def test_annual_agg(spark):
    result = annual_aggregation(_enriched(spark))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["year"] == 1995
    assert abs(rows[0]["avg_value"] - 5.0) < 0.001


def test_annual_excludes_quarterly(spark):
    result = annual_aggregation(_enriched(spark))
    assert "period" not in result.columns


def test_quarterly_ordered_by_year_period(spark):
    result = quarterly_aggregation(_enriched(spark))
    rows = result.collect()
    period_nums = [r["period_num"] for r in rows]
    assert period_nums == sorted(period_nums)


# ── population_stats — pyspark & sql ─────────────────────────────────────────

def _population(spark):
    return spark.createDataFrame([
        Row(nation_id="01000US", nation="United States", year=2013, population=316128839),
        Row(nation_id="01000US", nation="United States", year=2014, population=318857056),
        Row(nation_id="01000US", nation="United States", year=2015, population=321418821),
        Row(nation_id="01000US", nation="United States", year=2016, population=323127515),
        Row(nation_id="01000US", nation="United States", year=2017, population=325719178),
        Row(nation_id="01000US", nation="United States", year=2018, population=327167439),
    ])


def test_population_stats_mean(spark, gt):
    result = gt.population_stats(_population(spark)).collect()
    assert len(result) == 1
    assert abs(result[0]["mean_population"] - 322069808.0) < 1.0


def test_population_stats_stddev(spark, gt):
    result = gt.population_stats(_population(spark)).collect()
    assert result[0]["stddev_population"] > 0
    assert result[0]["stddev_population"] < 5_000_000


def test_population_stats_year_range(spark, gt):
    result = gt.population_stats(_population(spark)).collect()
    assert result[0]["year_from"] == 2013
    assert result[0]["year_to"] == 2018
    assert result[0]["year_count"] == 6


# ── best_year_per_series — pyspark & sql ──────────────────────────────────────

def _wide_two_series(spark):
    # PRS001: 1995 sum=3, 1996 sum=7 -> best year 1996
    # PRS002: 1995 sum=10, 1996 sum=1 -> best year 1995
    return spark.createDataFrame([
        Row(series_id="PRS001", year=1995, period="Q01", period_type="quarterly", value=1.0,
            series_label="Manufacturing — Employment, % Change (Seasonally Adjusted)"),
        Row(series_id="PRS001", year=1995, period="Q02", period_type="quarterly", value=2.0,
            series_label="Manufacturing — Employment, % Change (Seasonally Adjusted)"),
        Row(series_id="PRS001", year=1996, period="Q01", period_type="quarterly", value=3.0,
            series_label="Manufacturing — Employment, % Change (Seasonally Adjusted)"),
        Row(series_id="PRS001", year=1996, period="Q02", period_type="quarterly", value=4.0,
            series_label="Manufacturing — Employment, % Change (Seasonally Adjusted)"),
        Row(series_id="PRS002", year=1995, period="Q01", period_type="quarterly", value=10.0,
            series_label="Manufacturing — Employment, % Change (Not Seasonally Adjusted)"),
        Row(series_id="PRS002", year=1996, period="Q01", period_type="quarterly", value=1.0,
            series_label="Manufacturing — Employment, % Change (Not Seasonally Adjusted)"),
    ])


def test_best_year_correct(spark, gt):
    result = {r["series_id"]: r for r in gt.best_year_per_series(_wide_two_series(spark)).collect()}
    assert result["PRS001"]["year"] == 1996
    assert abs(result["PRS001"]["annual_sum"] - 7.0) < 0.001
    assert result["PRS002"]["year"] == 1995
    assert abs(result["PRS002"]["annual_sum"] - 10.0) < 0.001


def test_best_year_label_preserved(spark, gt):
    result = gt.best_year_per_series(_wide_two_series(spark)).collect()
    for row in result:
        assert "Manufacturing" in row["series_label"]
        assert "Employment" in row["series_label"]


def test_best_year_seasonal_label(spark, gt):
    result = {r["series_id"]: r for r in gt.best_year_per_series(_wide_two_series(spark)).collect()}
    assert "Seasonally Adjusted" in result["PRS001"]["series_label"]
    assert "Not Seasonally Adjusted" in result["PRS002"]["series_label"]


def test_best_year_one_row_per_series(spark, gt):
    result = gt.best_year_per_series(_wide_two_series(spark)).collect()
    series_ids = [r["series_id"] for r in result]
    assert len(series_ids) == len(set(series_ids))


# ── series_q01_with_population — pyspark & sql ────────────────────────────────

def _wide_with_label(spark):
    return spark.createDataFrame([
        Row(series_id="PRS001", series_label="Manufacturing — Employment (Seasonally Adjusted)",
            year=2013, period="Q01", period_type="quarterly", value=1.5),
        Row(series_id="PRS001", series_label="Manufacturing — Employment (Seasonally Adjusted)",
            year=2020, period="Q01", period_type="quarterly", value=-7.0),  # no population row
        Row(series_id="PRS001", series_label="Manufacturing — Employment (Seasonally Adjusted)",
            year=2013, period="Q02", period_type="quarterly", value=2.0),   # non-Q01, excluded
    ])


def test_q01_filters_to_q01_only(spark, gt):
    result = gt.series_q01_with_population(_wide_with_label(spark), _population(spark)).collect()
    assert all(r["period"] == "Q01" for r in result)
    assert len(result) == 2  # 2013 and 2020


def test_q01_population_joined(spark, gt):
    result = {r["year"]: r for r in gt.series_q01_with_population(_wide_with_label(spark), _population(spark)).collect()}
    assert result[2013]["us_population"] == 316128839
    assert result[2020]["us_population"] is None  # 2020 absent from ACS data


# ── build_wide_table: NULL seasonal label ─────────────────────────────────────

def test_wide_table_null_seasonal_label(spark):
    """A series with no match in silver.series produces NULL seasonal; label must be '(Unknown)', not '(Not Seasonally Adjusted)'."""
    data = spark.createDataFrame([
        Row(series_id="ORPHAN", year=2020, period="Q01", value=1.0, footnote_codes="",
            period_type="quarterly", period_num=1),
    ])
    empty_series = spark.createDataFrame([], _series(spark).schema)
    result = build_wide_table(
        data=data,
        series=empty_series,
        measure=_measure(spark),
        sector=_sector(spark),
        class_=_class(spark),
        duration=_duration(spark),
        seasonal=_seasonal(spark),
        period=_period_lookup(spark),
    )
    label = result.collect()[0]["series_label"]
    assert "(Unknown)" in label
    assert "Not Seasonally Adjusted" not in label
