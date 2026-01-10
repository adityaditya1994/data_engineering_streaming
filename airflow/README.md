# Airflow Orchestration for Clickstream KPI ETL

## Overview
This DAG launches three EMR PySpark steps for generating clickstream KPIs:
- Sensor Health (faulty %, active/inactive/faulty counts)
- Temperature & Humidity (aggregate stats by location/time)
- Event/Geo KPIs (event frequencies, sensor geo faults)

## Structure
- **DAG:** `clickstream_emr_kpis_dag.py`
- **boto3-only:** Uses PythonOperator with custom helper, not Airflow's EMR operator
- **Step Submission:** All PySpark scripts must be present on an S3 bucket accessible to EMR (set `SCRIPT_BUCKET` env)

## Required Environment Variables
- `EMR_CLUSTER_ID`: Cluster to submit steps to
- `SCRIPT_BUCKET`: S3 bucket containing PySpark scripts
- `BRONZE_PATH`: S3 raw clickstream json path
- `ICEBERG_DB`: Iceberg DB name for outputs
- `EMR_LOG_URI`: S3 path for EMR log output

## How to Trigger
Trigger the DAG from the UI or CLI. Each step runs in sequence, depends on its predecessor's success.

## Helper Scripts
- `emr_boto_helper.py`: Handles boto3 EMR step submission
- Edit DAG or pass parameters to customize script or table names.

