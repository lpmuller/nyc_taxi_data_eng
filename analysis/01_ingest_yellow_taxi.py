# Databricks notebook source
# Bronze ingestion entrypoint.

from src.config.settings import (
    BRONZE_SCHEMA,
    BRONZE_TABLE,
    CATALOG,
    RAW_UPLOAD_PATH,
    LANDING_PATH,
)

from src.bronze.ingest_yellow_taxi import (
    organize_landing_files,
    build_bronze_df,
    write_bronze_table,
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

organize_landing_files(
    dbutils=dbutils,
    raw_upload_path=RAW_UPLOAD_PATH,
    landing_path=LANDING_PATH,
)

bronze_df = build_bronze_df(
    spark=spark,
    dbutils=dbutils,
    landing_path=LANDING_PATH,
)

write_bronze_table(
    bronze_df=bronze_df,
    table_name=BRONZE_TABLE,
)
