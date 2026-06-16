from datetime import datetime

from pyspark.sql import SparkSession

from src.silver.transform_yellow_taxi import build_silver_df


def test_build_silver_df_filters_invalid_trips():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-transform")
        .getOrCreate()
    )

    input_data = [
        # valid trip
        (
            1,
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 15, 0),
            1.0,
            20.0,
            2,
            3.5,
            "file.parquet",
            datetime(2023, 1, 1, 0, 0, 0),
        ),
        # outside expected period
        (
            1,
            datetime(2023, 6, 1, 10, 0, 0),
            datetime(2023, 6, 1, 10, 15, 0),
            1.0,
            20.0,
            2,
            3.5,
            "file.parquet",
            datetime(2023, 1, 1, 0, 0, 0),
        ),
        # invalid duration
        (
            1,
            datetime(2023, 1, 1, 10, 15, 0),
            datetime(2023, 1, 1, 10, 0, 0),
            1.0,
            20.0,
            2,
            3.5,
            "file.parquet",
            datetime(2023, 1, 1, 0, 0, 0),
        ),
        # zero distance and duration lower than 1 minute
        (
            1,
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 0, 30),
            1.0,
            20.0,
            2,
            0.0,
            "file.parquet",
            datetime(2023, 1, 1, 0, 0, 0),
        ),
    ]

    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "total_amount",
        "payment_type",
        "trip_distance",
        "source_file",
        "ingestion_timestamp",
    ]

    bronze_df = spark.createDataFrame(input_data, columns)

    silver_df = build_silver_df(bronze_df)

    assert silver_df.count() == 1

    row = silver_df.collect()[0]

    assert row.VendorID == 1
    assert row.has_valid_passenger_count is True
    assert row.has_valid_distance is True
    assert row.has_negative_amount is False
