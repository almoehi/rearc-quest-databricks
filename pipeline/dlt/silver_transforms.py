from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DateType, IntegerType, StringType


def clean_strings(df: DataFrame) -> DataFrame:
    """TRIM all string columns and convert empty strings to NULL."""
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.expr(f"NULLIF(TRIM(`{col}`), '')"))
    return df


def to_upper(df: DataFrame, *cols: str) -> DataFrame:
    """UPPER-case the specified columns for consistent enum/code representation."""
    for col in cols:
        df = df.withColumn(col, F.upper(F.col(col)))
    return df


def deduplicate(
    df: DataFrame,
    pk_cols: list[str],
    tiebreak_col: str | None = None,
    prefer_value: str | None = None,
) -> DataFrame:
    """Keep one row per primary key.

    When tiebreak_col and prefer_value are provided, rows where tiebreak_col == prefer_value
    are ranked first (e.g. revised BLS estimates with footnote_codes='R' win over originals).
    Both parameters must be supplied together or not at all.
    """
    if (tiebreak_col is None) != (prefer_value is None):
        raise ValueError("tiebreak_col and prefer_value must both be provided, or both be None")
    if tiebreak_col is not None:
        order_expr = F.when(F.col(tiebreak_col) == prefer_value, F.lit(0)).otherwise(F.lit(1))
    else:
        order_expr = F.lit(0)
    w = Window.partitionBy(*pk_cols).orderBy(order_expr)
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .where(F.col("_rn") == 1)
          .drop("_rn")
    )


def schema_data(df: DataFrame) -> DataFrame:
    """Apply the full schema for bronze.raw_data: cast year→INT and value→DOUBLE;
    derive period_start_date, period_type, and period_num from the BLS period code."""
    period_start_date = (
        F.when(F.col("period") == "Q01", F.make_date(F.col("year"), F.lit(1), F.lit(1)))
         .when(F.col("period") == "Q02", F.make_date(F.col("year"), F.lit(4), F.lit(1)))
         .when(F.col("period") == "Q03", F.make_date(F.col("year"), F.lit(7), F.lit(1)))
         .when(F.col("period") == "Q04", F.make_date(F.col("year"), F.lit(10), F.lit(1)))
         .when(F.col("period") == "A01", F.make_date(F.col("year"), F.lit(1), F.lit(1)))
         .otherwise(F.lit(None).cast(DateType()))
    )
    return (
        df.withColumn("year", F.expr("TRY_CAST(year AS INT)"))
          .withColumn("value", F.expr("TRY_CAST(value AS DOUBLE)"))
          .withColumn("period_start_date", period_start_date)
          .withColumn(
              "period_type",
              F.when(F.col("period").startswith("Q"), F.lit("quarterly"))
               .when(F.col("period").startswith("A"), F.lit("annual"))
               .otherwise(F.lit("other")),
          )
          .withColumn(
              "period_num",
              F.regexp_extract(F.col("period"), r"\d+", 0).cast(IntegerType()),
          )
    )


def schema_series(df: DataFrame) -> DataFrame:
    """Apply the full schema for bronze.raw_series: cast base_year, begin_year, and end_year to INT."""
    return (
        df.withColumn("base_year", F.expr("TRY_CAST(base_year AS INT)"))
          .withColumn("begin_year", F.expr("TRY_CAST(begin_year AS INT)"))
          .withColumn("end_year", F.expr("TRY_CAST(end_year AS INT)"))
    )


def schema_population(df: DataFrame) -> DataFrame:
    """Apply the full schema for bronze.raw_population: cast year→INT and population→LONG.

    Two-stage cast via DOUBLE because the ACS API returns population as a float string
    (e.g. "321418821.0"). Both stages use TRY_CAST so malformed values yield NULL rather
    than raising under Databricks Runtime ANSI mode.
    """
    return (
        df.withColumn("year", F.expr("TRY_CAST(year AS INT)"))
          .withColumn("population", F.expr("TRY_CAST(TRY_CAST(population AS DOUBLE) AS BIGINT)"))
    )
