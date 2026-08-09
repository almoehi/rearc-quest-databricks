# Ancillary aggregations are not one of the three analytical questions —
# re-export the PySpark implementations unchanged.
from gold_transforms import annual_aggregation, quarterly_aggregation  # noqa: F401
from pyspark.sql import DataFrame


def population_stats(df: DataFrame) -> DataFrame:
    """Mean and standard deviation of annual US population; one row per nation. SQL variant."""
    return df.sparkSession.sql(
        """
        SELECT nation_id,
               nation,
               AVG(population)        AS mean_population,
               STDDEV_POP(population) AS stddev_population,
               MIN(year)              AS year_from,
               MAX(year)              AS year_to,
               COUNT(*)               AS year_count
        FROM   {df}
        GROUP BY nation_id, nation
        """,
        df=df,
    )


def best_year_per_series(wide: DataFrame) -> DataFrame:
    """Per series_id, the year whose quarterly values sum highest, with a human-readable label. SQL variant.

    series_label is read directly from the wide table — no additional joins needed.
    """
    return wide.sparkSession.sql(
        """
        WITH quarterly_sums AS (
            SELECT series_id, series_label, year, SUM(value) AS annual_sum
            FROM   {wide}
            WHERE  period_type = 'quarterly'
            GROUP BY series_id, series_label, year
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY annual_sum DESC) AS rn
            FROM   quarterly_sums
        )
        SELECT series_id, series_label, year, annual_sum
        FROM   ranked
        WHERE  rn = 1
        """,
        wide=wide,
    )


def series_q01_with_population(wide: DataFrame, population: DataFrame) -> DataFrame:
    """Q01 values for every series left-joined with that year's US population. SQL variant.

    series_label is read directly from the wide table — no additional joins needed.
    """
    return wide.sparkSession.sql(
        """
        SELECT /*+ BROADCAST(p) */
               w.series_id,
               w.series_label,
               w.year,
               w.period,
               w.value,
               p.population AS us_population
        FROM   (SELECT series_id, series_label, year, period, value
                FROM   {wide}
                WHERE  period = 'Q01') w
        LEFT JOIN {population} p ON w.year = p.year
        """,
        wide=wide,
        population=population,
    )
