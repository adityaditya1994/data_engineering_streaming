import boto3

def submit_emr_spark_step(
    emr_cluster_id: str,
    script_s3_path: str,
    input_path: str,
    iceberg_db: str,
    iceberg_tbl: str,
    region: str = 'eu-north-1',
    job_name: str = 'SparkJob'
):
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
                iceberg_tbl
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
#    iceberg_tbl='sensor_health'
# )

