# Databricks notebook source
# Silver transformation entrypoint.

from pyspark.sql.functions import col, count, max, min, when

from src.config.settings import BRONZE_TABLE, CATALOG, SILVER_SCHEMA, SILVER_TABLE
from src.silver.transform_yellow_taxi import build_silver_df, write_silver_table

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

bronze_df = spark.table(BRONZE_TABLE)
silver_final_df = build_silver_df(bronze_df)
write_silver_table(silver_df=silver_final_df, table_name=SILVER_TABLE)

silver_df = spark.table(SILVER_TABLE)

display(silver_df.limit(10))

display(
    silver_df.select(
        count("*").alias("total_records"),
        count(when(col("VendorID").isNull(), True)).alias("null_vendor_id"),
        count(when(col("total_amount").isNull(), True)).alias("null_total_amount"),
        count(when(col("tpep_pickup_datetime").isNull(), True)).alias("null_pickup_datetime"),
        count(when(col("tpep_dropoff_datetime").isNull(), True)).alias("null_dropoff_datetime"),
        count(when(col("pickup_date").isNull(), True)).alias("null_pickup_date"),
        count(when(col("pickup_hour").isNull(), True)).alias("null_pickup_hour"),
    )
)

display(
    silver_df.select(
        min("pickup_date").alias("min_pickup_date"),
        max("pickup_date").alias("max_pickup_date"),
        min("trip_duration_minutes").alias("min_trip_duration_minutes"),
        max("trip_duration_minutes").alias("max_trip_duration_minutes"),
        min("total_amount").alias("min_total_amount"),
        max("total_amount").alias("max_total_amount"),
        min("trip_distance").alias("min_trip_distance"),
        max("trip_distance").alias("max_trip_distance"),
    )
)

display(
    silver_df.select(
        count(when(col("has_passenger_information") == False, True)).alias("missing_passenger_information"),
        count(when(col("has_valid_passenger_count") == False, True)).alias("invalid_passenger_count_flagged"),
        count(when(col("has_negative_amount") == True, True)).alias("negative_amount_flagged"),
        count(when(col("has_valid_distance") == False, True)).alias("zero_distance_flagged"),
        count(when(col("is_total_amount_outlier") == True, True)).alias("amount_outliers_flagged"),
    )
)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
removed_records = bronze_count - silver_count
removed_percentage = removed_records / bronze_count * 100
retention_percentage = silver_count / bronze_count * 100

print(f"Bronze records: {bronze_count}")
print(f"Silver records: {silver_count}")
print(f"Removed records: {removed_records}")
print(f"Removed percentage: {removed_percentage:.2f}%")
print(f"Retention percentage: {retention_percentage:.2f}%")
