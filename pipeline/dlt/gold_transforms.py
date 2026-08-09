from pyspark.sql import DataFrame, Window, functions as F


def build_wide_table(
    data: DataFrame,
    series: DataFrame,
    measure: DataFrame,
    sector: DataFrame,
    class_: DataFrame,
    duration: DataFrame,
    seasonal: DataFrame,
    period: DataFrame,
) -> DataFrame:
    """Join silver.data with all dimension tables into a single enriched wide table.

    series_label is derived here so all downstream analytics can consume it directly
    without repeating the label construction logic.
    """
    series_slim = series.select("series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal")

    result = (
        data
        .join(F.broadcast(series_slim), on="series_id", how="left")
        .join(F.broadcast(measure.select("measure_code", "measure_text")), on="measure_code", how="left")
        .join(F.broadcast(sector.select("sector_code", "sector_name")), on="sector_code", how="left")
        .join(F.broadcast(class_.select("class_code", "class_text")), on="class_code", how="left")
        .join(F.broadcast(duration.select("duration_code", "duration_text")), on="duration_code", how="left")
        .join(
            F.broadcast(seasonal.select(F.col("seasonal_code").alias("seasonal"), "seasonal_text")),
            on="seasonal",
            how="left",
        )
        .join(F.broadcast(period.select("period", "period_abbr", "period_name")), on="period", how="left")
    )

    seasonal_label = (
        F.when(F.col("seasonal") == "S", F.lit("Seasonally Adjusted"))
         .when(F.col("seasonal") == "U", F.lit("Not Seasonally Adjusted"))
         .otherwise(F.lit("Unknown"))
    )
    return result.withColumn(
        "series_label",
        F.concat(
            F.coalesce(F.col("sector_name"), F.lit("Unknown sector")),
            F.lit(" — "),
            F.coalesce(F.col("measure_text"), F.lit("Unknown measure")),
            F.lit(", "),
            F.coalesce(F.col("duration_text"), F.lit("Unknown duration")),
            F.lit(" ("),
            seasonal_label,
            F.lit(")"),
        ),
    )


def quarterly_aggregation(df: DataFrame) -> DataFrame:
    """Aggregate enriched data by sector, measure, year, and quarter."""
    return (
        df.where(F.col("period_type") == "quarterly")
          .groupBy(
              "sector_code", "sector_name",
              "measure_code", "measure_text",
              "year", "period", "period_num",
          )
          .agg(
              F.avg("value").alias("avg_value"),
              F.min("value").alias("min_value"),
              F.max("value").alias("max_value"),
              F.count("*").alias("series_count"),
          )
    )


def annual_aggregation(df: DataFrame) -> DataFrame:
    """Aggregate enriched data by sector, measure, and year (annual rows only)."""
    return (
        df.where(F.col("period_type") == "annual")
          .groupBy(
              "sector_code", "sector_name",
              "measure_code", "measure_text",
              "year",
          )
          .agg(
              F.avg("value").alias("avg_value"),
              F.min("value").alias("min_value"),
              F.max("value").alias("max_value"),
              F.count("*").alias("series_count"),
          )
    )


def population_stats(df: DataFrame) -> DataFrame:
    """Mean and standard deviation of annual US population; one row per nation."""
    return (
        df.groupBy("nation_id", "nation")
          .agg(
              F.mean("population").alias("mean_population"),
              F.stddev_pop("population").alias("stddev_population"),
              F.min("year").alias("year_from"),
              F.max("year").alias("year_to"),
              F.count("*").alias("year_count"),
          )
    )


def best_year_per_series(wide: DataFrame) -> DataFrame:
    """For each series_id, find the year with the highest sum of quarterly values.

    series_label is read directly from the wide table — no additional joins needed.
    """
    quarterly_sums = (
        wide
        .where(F.col("period_type") == "quarterly")
        .groupBy("series_id", "series_label", "year")
        .agg(F.sum("value").alias("annual_sum"))
    )
    w = Window.partitionBy("series_id").orderBy(F.col("annual_sum").desc())
    return (
        quarterly_sums
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
        .select("series_id", "series_label", "year", "annual_sum")
    )


def series_q01_with_population(wide: DataFrame, population: DataFrame) -> DataFrame:
    """Q01 values for every series joined with that year's US population where available.

    series_label is read directly from the wide table — no additional joins needed.
    Population is left-joined on year so years without ACS data (e.g. 2020) still appear.
    """
    pop = population.select("year", F.col("population").alias("us_population"))
    return (
        wide
        .where(F.col("period") == "Q01")
        .join(F.broadcast(pop), on="year", how="left")
        .select("series_id", "series_label", "year", "period", "value", "us_population")
    )
