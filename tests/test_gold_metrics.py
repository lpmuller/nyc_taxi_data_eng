from datetime import date, datetime

from pyspark.sql import SparkSession

from src.gold.create_metrics import (
    create_hourly_passenger_metric,
    create_monthly_amount_metric,
)


def test_create_monthly_amount_metric():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-monthly")
        .getOrCreate()
    )

    input_data = [
        (date(2023, 1, 1), 10.0),
        (date(2023, 1, 2), 20.0),
        (date(2023, 2, 1), 30.0),
    ]

    df = spark.createDataFrame(input_data, ["pickup_date", "total_amount"])

    result = create_monthly_amount_metric(df).collect()

    result_by_month = {
        row.pickup_month.strftime("%Y-%m"): row.avg_total_amount for row in result
    }

    assert result_by_month["2023-01"] == 15.0
    assert result_by_month["2023-02"] == 30.0


def test_create_hourly_passenger_metric_filters_invalid_passengers():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-hourly")
        .getOrCreate()
    )

    input_data = [
        (10, 1.0, True),
        (10, 3.0, True),
        (10, None, False),
        (11, 2.0, True),
    ]

    df = spark.createDataFrame(
        input_data,
        [
            "pickup_hour",
            "passenger_count",
            "has_valid_passenger_count",
        ],
    )

    result = create_hourly_passenger_metric(df).collect()

    result_by_hour = {row.pickup_hour: row.avg_passenger_count for row in result}

    assert result_by_hour[10] == 2.0
    assert result_by_hour[11] == 2.0
