# Databricks notebook source
# Gold metrics entrypoint.

from src.config.settings import CATALOG, GOLD_SCHEMA, HOURLY_PASSENGER_TABLE, MONTHLY_AMOUNT_TABLE, SILVER_TABLE
from src.gold.create_metrics import create_hourly_passenger_metric, create_monthly_amount_metric, write_gold_table

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

silver_df = spark.table(SILVER_TABLE)
monthly_amount_df = create_monthly_amount_metric(silver_df)
hourly_passenger_df = create_hourly_passenger_metric(silver_df)

write_gold_table(monthly_amount_df, MONTHLY_AMOUNT_TABLE)
write_gold_table(hourly_passenger_df, HOURLY_PASSENGER_TABLE)

display(spark.table(MONTHLY_AMOUNT_TABLE))
display(spark.table(HOURLY_PASSENGER_TABLE))
