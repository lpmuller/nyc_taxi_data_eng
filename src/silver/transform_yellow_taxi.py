"""Silver transformation pipeline for NYC Yellow Taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, hour, to_date, unix_timestamp


def build_silver_df(bronze_df: DataFrame) -> DataFrame:
    """Transform Bronze data into a curated Silver consumption table."""
    silver_base_df = (
        bronze_df
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60,
        )
    )
    silver_clean_df = (
        silver_base_df
        .filter((col("tpep_pickup_datetime") >= "2023-01-01") & (col("tpep_pickup_datetime") < "2023-06-01"))
        .filter(col("trip_duration_minutes") > 0)
        .filter(~((col("trip_distance") == 0) & (col("trip_duration_minutes") < 1)))
    )
    amount_threshold = silver_clean_df.approxQuantile("total_amount", [0.995], 0.001)[0]
    return (
        silver_clean_df
        .withColumn("has_passenger_information", col("passenger_count").isNotNull())
        .withColumn("has_valid_passenger_count", col("passenger_count").isNotNull() & (col("passenger_count") > 0))
        .withColumn("has_negative_amount", col("total_amount") < 0)
        .withColumn("is_disputed_trip", col("payment_type") == 4)
        .withColumn("has_valid_distance", col("trip_distance") > 0)
        .withColumn("is_total_amount_outlier", col("total_amount") > amount_threshold)
        .withColumn("updated_at", current_timestamp())
        .select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime"),
            col("pickup_date"),
            col("pickup_hour"),
            col("trip_duration_minutes"),
            col("payment_type"),
            col("trip_distance"),
            col("has_passenger_information"),
            col("has_valid_passenger_count"),
            col("has_negative_amount"),
            col("is_disputed_trip"),
            col("has_valid_distance"),
            col("is_total_amount_outlier"),
            col("source_file"),
            col("ingestion_timestamp"),
            col("updated_at"),
        )
    )


def write_silver_table(silver_df: DataFrame, table_name: str) -> None:
    """Write Silver DataFrame as a Delta table partitioned by pickup date."""
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_date")
        .saveAsTable(table_name)
    )
