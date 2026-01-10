# Clickstream ETL (Airflow + EMR + Iceberg, boto3 style)

## Stack Overview
- **Airflow** on EC2: Orchestrator, submits jobs via Python/boto3
- **EMR**: Executes PySpark KPI scripts
- **S3 Bronze**: Raw clickstream landing
- **S3 Iceberg**: KPIs in analytical/partitioned format
- **CloudFormation**: Spin up Airflow EC2 with correct IAM

## Quick Start
1. Deploy `Cloudformations/ec2_airflow_emr.yaml` using CloudFormation console. Supply `KeyName` parameter (existing EC2 SSH key).
2. Upload PySpark scripts under `EMR/Pyspark/` to your S3 script bucket.
3. Configure Airflow environment variables:
   - `EMR_CLUSTER_ID`, `SCRIPT_BUCKET`, `BRONZE_PATH`, `ICEBERG_DB`, `EMR_LOG_URI`
4. Trigger `clickstream_emr_kpis` DAG from the Airflow UI.

## KPI Scripts
See `EMR/Pyspark/README.md` for details, arguments, & local run example.

## Boto3 EMR Submission
No Airflow native EMR operator. See `airflow/emr_boto_helper.py` used by DAG.

## Sample Data
Sample packet & test dataset in `EMR/Pyspark/sample_clickstream.json`.

---
Edit EMR cluster, S3 buckets, and Iceberg catalog for your org’s topology. Tables will be auto-created.

