import boto3
import os

def submit_emr_spark_step(
    emr_cluster_id: str,
    script_s3_path: str,
    input_path: str,
    iceberg_db: str,
    iceberg_tbl: str,
    iceberg_warehouse: str = None,
    region: str = 'eu-north-1',
    job_name: str = 'SparkJob'
):
    """
    Submit a Spark step to an EMR cluster.
    
    Args:
        emr_cluster_id: The EMR cluster ID
        script_s3_path: S3 path to the PySpark script
        input_path: S3 path to input data (bronze bucket)
        iceberg_db: Glue database name (e.g., 'analytics')
        iceberg_tbl: Iceberg table name
        iceberg_warehouse: S3 path for Iceberg warehouse (e.g., 's3://bucket/')
        region: AWS region
        job_name: Name for the EMR step
    """
    # Default iceberg_warehouse from environment if not provided
    if iceberg_warehouse is None:
        iceberg_warehouse = f"s3://{os.environ.get('ICEBERG_BUCKET', 'your-iceberg-bucket')}/"
    
    client = boto3.client('emr', region_name=region)
    step = {
        'Name': job_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                script_s3_path,
                input_path,
                iceberg_db,
                iceberg_tbl,
                iceberg_warehouse  # 4th argument for PySpark scripts
            ]
        }
    }
    response = client.add_job_flow_steps(
        JobFlowId=emr_cluster_id,
        Steps=[step]
    )
    return response['StepIds'][0]

# Usage Example (in Airflow operator call):
# from emr_boto_helper import submit_emr_spark_step
# step_id = submit_emr_spark_step(
#    emr_cluster_id=os.environ['EMR_CLUSTER_ID'],
#    script_s3_path='s3://bucket/kpi_sensor_health.py',
#    input_path='s3://bronze/input/',
#    iceberg_db='analytics',
#    iceberg_tbl='sensor_health',
#    iceberg_warehouse='s3://iceberg-bucket/'
# )
