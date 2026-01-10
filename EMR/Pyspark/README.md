# PySpark KPI Scripts for Clickstream ETL

## Overview
Contains ETL scripts to read clickstream data from S3 (bronze layer), compute KPIs, and write to S3 as Apache Iceberg tables. Scripts auto-create schemas/tables.

## Scripts
- `kpi_sensor_health.py`: Computes per-sensor health/faulty rates, overall status counts.
- `kpi_temp_humidity.py`: Temperature/humidity stats per day/location.
- `kpi_event_geo.py`: Event type frequencies & geolocation-based faulty sensor counts.

### Arguments (for all scripts)
- `$1`: Input S3 path (e.g., `s3://my-bronze/input/`)
- `$2`: Iceberg DB (e.g., `analytics`)
- `$3`: Iceberg table/stem (e.g., `sensor_health`)

### Example Run (local Spark, using sample)
```bash
spark-submit kpi_sensor_health.py sample_clickstream.json analytics sensor_health
```

## Sample Packet
See `sample_clickstream.json` with synthetic data. Modify as needed for testing.

---

# Airflow & EMR
Your Airflow DAG (see `../airflow/clickstream_emr_kpis_dag.py`) uses boto3 to submit these scripts as steps to the EMR cluster. Scripts must be uploaded to an S3 bucket (see EMR "script_bucket").

