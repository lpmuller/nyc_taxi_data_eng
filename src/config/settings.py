"""Project configuration for the iFood Data Engineering case."""

CATALOG = "workspace"

LANDING_VOLUME_SCHEMA = "ifood_case"
LANDING_VOLUME_NAME = "landing"

RAW_UPLOAD_PATH = "s3://ifood-case-laura/raw_upload/"
LANDING_PATH = (
    f"/Volumes/{CATALOG}/{LANDING_VOLUME_SCHEMA}/{LANDING_VOLUME_NAME}/yellow_taxi/"
)

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.yellow_taxi_trip"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.yellow_taxi_trip"
MONTHLY_AMOUNT_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.yellow_taxi_monthly_amount"
HOURLY_PASSENGER_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.yellow_taxi_hourly_passenger"
