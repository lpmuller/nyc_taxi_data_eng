"""Bronze ingestion pipeline for NYC Yellow Taxi data."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, regexp_extract

import re


def organize_landing_files(dbutils, raw_upload_path: str, landing_path: str) -> None:
    """Copy raw files from S3 to Unity Catalog Volume organized by year/month."""
    files = [
        file
        for file in dbutils.fs.ls(raw_upload_path)
        if file.name.endswith(".parquet")
    ]

    if not files:
        raise ValueError(f"No parquet files found in raw path: {raw_upload_path}")

    for file in files:
        match = re.search(
            r"yellow_tripdata_(\d{4})-(\d{2})",
            file.name
        )

        if not match:
            print(f"Skipping invalid file name: {file.name}")
            continue

        year = match.group(1)
        month = match.group(2)

        destination = (
            f"{landing_path}"
            f"year={year}/"
            f"month={month}/"
            f"{file.name}"
        )

        print(f"Copying {file.path} -> {destination}")

        dbutils.fs.cp(
            file.path,
            destination
        )


def list_parquet_files(dbutils, landing_path: str) -> list[str]:
    """List parquet files stored in the organized landing zone."""
    return [
        file.path
        for month_dir in dbutils.fs.ls(landing_path + "year=2023/")
        for file in dbutils.fs.ls(month_dir.path)
        if file.name.endswith(".parquet")
    ]


def read_and_standardize_file(spark, path: str) -> DataFrame:
    """Read one parquet file and cast source columns to a common schema.

    NYC TLC monthly parquet files may present schema drift across months.
    Each file is read individually using its own schema, then relevant columns
    are explicitly cast before unioning all files.
    """
    return (
        spark.read.parquet(path)
        .select(
            col("VendorID").cast("long").alias("VendorID"),
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime"),
            col("passenger_count").cast("double").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("RatecodeID").cast("double").alias("RatecodeID"),
            col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            col("PULocationID").cast("long").alias("PULocationID"),
            col("DOLocationID").cast("long").alias("DOLocationID"),
            col("payment_type").cast("long").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("extra").cast("double").alias("extra"),
            col("mta_tax").cast("double").alias("mta_tax"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            col("total_amount").cast("double").alias("total_amount"),
            col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
            col("airport_fee").cast("double").alias("airport_fee"),
        )
        .withColumn("source_file", lit(path))
    )


def build_bronze_df(spark, dbutils, landing_path: str) -> DataFrame:
    """Build the Bronze DataFrame from landing parquet files."""
    files = list_parquet_files(dbutils, landing_path)
    if not files:
        raise ValueError(f"No parquet files found in landing path: {landing_path}")
    dfs = [read_and_standardize_file(spark, path) for path in files]
    bronze_df = reduce(DataFrame.unionByName, dfs)
    return (
        bronze_df
        .withColumn("year", regexp_extract(col("source_file"), r"year=(\d{4})", 1).cast("int"))
        .withColumn("month", regexp_extract(col("source_file"), r"month=(\d{2})", 1).cast("int"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def write_bronze_table(bronze_df: DataFrame, table_name: str) -> None:
    """Write Bronze DataFrame as a partitioned Delta table."""
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month")
        .saveAsTable(table_name)
    )
