from datetime import date, datetime

from pyspark.sql import SparkSession

from src.gold.create_metrics import (
    create_may_hourly_passenger_metric,
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


def test_create_may_hourly_passenger_metric_filters_may_and_invalid_passengers():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-may-hourly")
        .getOrCreate()
    )

    input_data = [
        # valid May records - should be included
        (
            date(2023, 5, 1),
            10,
            1.0,
            True,
        ),
        (
            date(2023, 5, 2),
            10,
            3.0,
            True,
        ),
        # invalid passenger information - should be ignored
        (
            date(2023, 5, 3),
            10,
            None,
            False,
        ),
        # different hour - should create another group
        (
            date(2023, 5, 4),
            11,
            2.0,
            True,
        ),
        # outside May - should be ignored
        (
            date(2023, 4, 30),
            10,
            10.0,
            True,
        ),
        (
            date(2023, 6, 1),
            10,
            10.0,
            True,
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        [
            "pickup_date",
            "pickup_hour",
            "passenger_count",
            "has_valid_passenger_count",
        ],
    )

    result = create_may_hourly_passenger_metric(df).collect()

    result_by_hour = {
        row.pickup_hour: {
            "avg": row.avg_passenger_count,
            "records": row.valid_passenger_records,
        }
        for row in result
    }

    assert len(result_by_hour) == 2

    assert result_by_hour[10]["avg"] == 2.0
    assert result_by_hour[10]["records"] == 2

    assert result_by_hour[11]["avg"] == 2.0
    assert result_by_hour[11]["records"] == 1
