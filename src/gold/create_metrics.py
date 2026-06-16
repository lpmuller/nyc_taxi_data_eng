"""Gold metric creation for NYC Yellow Taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, current_timestamp, date_trunc, round


def create_monthly_amount_metric(silver_df: DataFrame) -> DataFrame:
    """Create monthly average total amount metric."""
    return (
        silver_df
        .withColumn("pickup_month", date_trunc("month", col("pickup_date")))
        .groupBy("pickup_month")
        .agg(round(avg("total_amount"), 2).alias("avg_total_amount"), count("*").alias("total_trips"))
        .withColumn("updated_at", current_timestamp())
        .orderBy("pickup_month")
    )


def create_hourly_passenger_metric(silver_df: DataFrame) -> DataFrame:
    """Create average passenger count by pickup hour metric."""
    return (
        silver_df
        .filter(col("has_valid_passenger_count"))
        .groupBy("pickup_hour")
        .agg(round(avg("passenger_count"), 2).alias("avg_passenger_count"), count("*").alias("valid_passenger_records"))
        .withColumn("updated_at", current_timestamp())
        .orderBy("pickup_hour")
    )


def write_gold_table(df: DataFrame, table_name: str) -> None:
    """Write a Gold DataFrame as a Delta table."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
