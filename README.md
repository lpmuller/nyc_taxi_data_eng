# NYC Taxi Data Engineering Case

This project implements a Lakehouse pipeline for NYC TLC Yellow Taxi data from January to May 2023 using Databricks, Spark and Delta Lake.

## Repository structure

```text
ifood-case/
├─ src/              # Reusable source code
├─ analysis/         # Databricks notebooks/scripts and SQL answers
├─ README.md
└─ requirements.txt
```

## Architecture

The solution follows the Medallion Architecture pattern:

```text
AWS S3 Raw Upload
        ↓
Databricks Landing Zone
        ↓
Bronze Delta Table
        ↓
Silver Delta Table
        ↓
Gold Delta Tables
```

The pipeline separates raw file storage, ingestion, data quality processing and business consumption layers.

---

# Data Source

The input data comes from the NYC TLC Yellow Taxi Trip Records dataset.

The original parquet files are stored in an AWS S3 bucket as downloaded from the source:

```text
s3://ifood-case-laura/raw_upload/

├── yellow_tripdata_2023-01.parquet
├── yellow_tripdata_2023-02.parquet
├── yellow_tripdata_2023-03.parquet
├── yellow_tripdata_2023-04.parquet
└── yellow_tripdata_2023-05.parquet
```

The bucket is used as an external raw source and remains read-only during the pipeline execution.

---

# Landing Zone

During ingestion, the pipeline copies the raw files from S3 into a Databricks Unity Catalog Volume and organizes them into a partitioned folder structure:

```text
/Volumes/workspace/ifood_case/landing/yellow_taxi/

├── year=2023/
│   ├── month=01/
│   │   └── yellow_tripdata_2023-01.parquet
│   ├── month=02/
│   │   └── yellow_tripdata_2023-02.parquet
│   ├── month=03/
│   ├── month=04/
│   └── month=05/
```

This approach keeps the external source immutable while allowing Databricks to manage the curated landing structure used by the pipeline.

### Bronze

The Bronze layer preserves the source data and adds ingestion metadata.

The monthly parquet files presented schema inconsistencies, so each file is read individually, standardized through explicit casts, and combined with `unionByName`.

### Silver

The Silver layer acts as the consumption layer for this case. It guarantees the columns required by the challenge:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

Additional raw attributes are preserved in Bronze. Silver adds analytical fields and quality flags.

Data profiling showed that not every unexpected value should be removed:

- Null `passenger_count` values were concentrated in Flex Fare trips.
- Negative `total_amount` values were concentrated mainly in Dispute and No Charge transactions.
- Zero-distance trips sometimes contained valid paid rides.

Therefore, only records with strong evidence of invalid trips were removed:

- Pickup outside Jan-May 2023
- Non-positive trip duration
- Zero distance combined with duration lower than 1 minute

Other scenarios were preserved with quality flags to avoid analytical bias.

Final validation:

```text
Bronze records: 16,186,386
Silver records: 16,066,369
Removed records: 120,017
Removed percentage: 0.74%
Retention percentage: 99.26%
```

### Gold

Gold tables provide business-ready metrics:

- `workspace.gold.yellow_taxi_monthly_amount`
- `workspace.gold.yellow_taxi_hourly_passenger`

## Gold results

### Average total amount by month

| Month | Average total amount | Total trips |
|---|---:|---:|
| 2023-01 | 26.98 | 3,043,879 |
| 2023-02 | 26.88 | 2,892,533 |
| 2023-03 | 27.79 | 3,378,343 |
| 2023-04 | 28.24 | 3,264,408 |
| 2023-05 | 28.94 | 3,487,206 |

### Average passenger count by hour in May 2023

Results are available in:

```sql
SELECT *
FROM workspace.gold.yellow_taxi_hourly_passenger
ORDER BY pickup_hour;
```

Only May 2023 trips with valid passenger information are used to answer the second question, as requested by the case.

## How to run

This project is intended to run inside Databricks.

Databricks provides:

- active Spark session: `spark`
- filesystem utilities: `dbutils`
- Unity Catalog access
- Delta table support

Clone the repository into Databricks Repos and execute the files in order:

```text
analysis/01_ingest_yellow_taxi.py (Change to Servless GPU before running)
analysis/02_transform_silver_yellow_taxi.py
analysis/03_create_gold_yellow_taxi_metrics.py
```

The `src/` package contains reusable pipeline logic. The files in `analysis/` act as lightweight Databricks entrypoints.

## SQL answers

The final SQL queries are available at:

```text
analysis/question_1_monthly_amount.sql
analysis/question_2_hourly_passenger.sql
```
## Production considerations:
- Orchestration with Databricks Workflows or Airflow
- Incremental ingestion by file/month
- Data quality checks with expectations
- Monitoring and alerting
- Infrastructure managed with Terraform
- Secrets handled through Databricks Secret Scope or cloud IAM
