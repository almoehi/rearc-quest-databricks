import dlt
from pyspark.sql.types import StructType, StructField, StringType

_S = StringType()
_bronze = "bronze"
_data_path = spark.conf.get("data_path")  # noqa: F821 — spark is a DLT runtime global


def _autoload(subdir: str, schema: StructType, glob_filter: str | None = None):
    """Read BLS tab-separated files from a subdirectory via Auto Loader.

    BLS files have whitespace-padded headers and values.
    enforceSchema=true maps columns positionally using schema names (ignores header padding).
    ignoreLeading/TrailingWhiteSpace trims cell values.

    glob_filter: restrict to a single filename within a shared directory (used for
    enrichment files that all live in enrichment/ but have distinct schemas).
    """
    schema_loc = f"{_data_path}/_schema/{subdir}"
    if glob_filter:
        schema_loc = f"{schema_loc}/{glob_filter}"

    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("enforceSchema", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", schema_loc)
        #.option("cloudFiles.includeExistingFiles", "true")
    )
    if glob_filter:
        reader = reader.option("pathGlobFilter", glob_filter)
    return reader.schema(schema).load(f"{_data_path}/{subdir}/")


_data_schema = StructType([
    StructField("series_id", _S),
    StructField("year", _S),
    StructField("period", _S),
    StructField("value", _S),
    StructField("footnote_codes", _S),
])


@dlt.table(
    name=f"{_bronze}.raw_data",
    comment="Raw BLS pr.data.*.AllData files ingested via Auto Loader; complete historical time series including all revisions; all columns are raw strings.",
    table_properties={"quality": "bronze"},
    cluster_by=["series_id", "year"],
)
def raw_data():
    return _autoload("backfill", _data_schema)


@dlt.table(
    name=f"{_bronze}.raw_series",
    comment="Raw BLS pr.series file mapping each series_id to its economic attributes (sector, class, measure, duration, seasonal adjustment); one row per unique BLS time series; all columns are raw strings.",
    table_properties={"quality": "bronze"},
    cluster_by=["series_id"],
)
def raw_series():
    return _autoload("series", StructType([
        StructField("series_id", _S),
        StructField("sector_code", _S),
        StructField("class_code", _S),
        StructField("measure_code", _S),
        StructField("duration_code", _S),
        StructField("seasonal", _S),
        StructField("base_year", _S),
        StructField("footnote_codes", _S),
        StructField("begin_year", _S),
        StructField("begin_period", _S),
        StructField("end_year", _S),
        StructField("end_period", _S),
    ]))


@dlt.table(
    name=f"{_bronze}.raw_measure",
    comment="Raw BLS pr.measure reference table mapping measure_code to a human-readable description of the economic quantity tracked (e.g., output per hour, unit labor costs); sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_measure():
    return _autoload("enrichment", StructType([
        StructField("measure_code", _S),
        StructField("measure_text", _S),
        StructField("display_level", _S),
        StructField("selectable", _S),
        StructField("sort_sequence", _S),
    ]), glob_filter="pr.measure")


@dlt.table(
    name=f"{_bronze}.raw_sector",
    comment="Raw BLS pr.sector reference table mapping sector_code to a human-readable sector name (e.g., nonfarm business, manufacturing); sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_sector():
    return _autoload("enrichment", StructType([
        StructField("sector_code", _S),
        StructField("sector_name", _S),
        StructField("display_level", _S),
        StructField("selectable", _S),
        StructField("sort_sequence", _S),
    ]), glob_filter="pr.sector")


@dlt.table(
    name=f"{_bronze}.raw_class",
    comment="Raw BLS pr.class reference table mapping class_code to a human-readable sub-classification within a sector; sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_class():
    return _autoload("enrichment", StructType([
        StructField("class_code", _S),
        StructField("class_text", _S),
        StructField("display_level", _S),
        StructField("selectable", _S),
        StructField("sort_sequence", _S),
    ]), glob_filter="pr.class")


@dlt.table(
    name=f"{_bronze}.raw_duration",
    comment="Raw BLS pr.duration reference table mapping duration_code to the type of change measured (e.g., percent change from same quarter one year ago, percent change from prior quarter); sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_duration():
    return _autoload("enrichment", StructType([
        StructField("duration_code", _S),
        StructField("duration_text", _S),
        StructField("display_level", _S),
        StructField("selectable", _S),
        StructField("sort_sequence", _S),
    ]), glob_filter="pr.duration")


@dlt.table(
    name=f"{_bronze}.raw_footnote",
    comment="Raw BLS pr.footnote reference table mapping footnote_code to its human-readable explanation; 'R' denotes a revised estimate superseding a prior release; sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_footnote():
    return _autoload("enrichment", StructType([
        StructField("footnote_code", _S),
        StructField("footnote_text", _S),
    ]), glob_filter="pr.footnote")


@dlt.table(
    name=f"{_bronze}.raw_period",
    comment="Raw BLS pr.period reference table mapping period codes (Q01–Q05, A01) to human-readable abbreviations and full names; sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_period():
    return _autoload("enrichment", StructType([
        StructField("period", _S),
        StructField("period_abbr", _S),
        StructField("period_name", _S),
    ]), glob_filter="pr.period")


@dlt.table(
    name=f"{_bronze}.raw_seasonal",
    comment="Raw BLS pr.seasonal reference table mapping seasonal adjustment codes to descriptions (S = seasonally adjusted, U = not seasonally adjusted); sourced from the BLS pr/ directory.",
    table_properties={"quality": "bronze"},
)
def raw_seasonal():
    return _autoload("enrichment", StructType([
        StructField("Seasonal_code", _S),
        StructField("Seasonal_text", _S),
    ]), glob_filter="pr.seasonal")


@dlt.table(
    name=f"{_bronze}.raw_population",
    comment="Raw US annual total population estimates sourced from the DataUSA ACS Tesseract API (cube acs_yg_total_population_1, drilldowns Year × Nation); one row per nation per year; all columns ingested as raw strings.",
    table_properties={"quality": "bronze"},
)
def raw_population():
    # enforceSchema=true maps columns positionally and ignores the CSV header, so we
    # can use clean snake_case names even though the source file has "Nation ID" etc.
    return _autoload("enrichment", StructType([
        StructField("nation_id", _S),
        StructField("nation", _S),
        StructField("year", _S),
        StructField("population", _S),
    ]), glob_filter="us_population")
