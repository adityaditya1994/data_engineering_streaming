from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import time
import json



def create_emr_cluster():

    client = boto3.client('emr', region_name='eu-north-1')
    # Cheapest config: 1 master + 2 core, both m5.xlarge as minimal for Iceberg + Glue
    response = client.run_job_flow(
        Name='kpi-teaching-emr',
        LogUri=os.environ['EMR_LOG_URI'],
        ReleaseLabel='emr-6.15.0',
        Applications=[{'Name': 'Spark'}],
        Instances={
            'InstanceGroups': [
                # Master node: On-Demand (stable)
                {'Name': 'Master nodes', 'Market': 'ON_DEMAND', 'InstanceRole': 'MASTER', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
                # Core nodes: Spot (cheaper)
                {'Name': 'Core nodes', 'Market': 'SPOT', 'BidPrice': '0.10', 'InstanceRole': 'CORE', 'InstanceType': 'm5.xlarge', 'InstanceCount': 2}
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        JobFlowRole=os.environ.get('EMR_EC2_ROLE', 'EMR_EC2_DefaultRole'),
        ServiceRole=os.environ.get('EMR_ROLE', 'EMR_DefaultRole'),
        VisibleToAllUsers=True,
        Configurations=[
            {
                'Classification': 'spark',
                'Properties': {'maximizeResourceAllocation': 'true'}
            },
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
                    'spark.sql.catalog.spark_catalog.type': 'hive',
                    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                    'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2'
                }
            },
            {
                'Classification': 'spark-hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }
        ]
    )
    cluster_id = response['JobFlowId']

    return cluster_id

def wait_for_cluster(cluster_id):
    client = boto3.client('emr', region_name='eu-north-1')
    state = None
    while state not in ('WAITING', 'RUNNING'):
        time.sleep(30)
        desc = client.describe_cluster(ClusterId=cluster_id)
        state = desc['Cluster']['Status']['State']


    return state

def submit_emr_job(kpi_script, job_name, s3_bronze, iceberg_db, iceberg_tbl, iceberg_warehouse, emr_cluster_id, emr_log_uri, script_bucket, **context):

    client = boto3.client('emr', region_name='eu-north-1')
    step = {
        'Name': job_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                f's3://{script_bucket}/{kpi_script}',
                s3_bronze,
                iceberg_db,
                iceberg_tbl,
                iceberg_warehouse  # 4th argument: Iceberg warehouse S3 path
            ]
        }
    }
    response = client.add_job_flow_steps(
        JobFlowId=emr_cluster_id,
        Steps=[step]
    )
    step_id = response['StepIds'][0]
    
    # Poll for step completion
    while True:
        status_details = client.describe_step(ClusterId=emr_cluster_id, StepId=step_id)
        step_state = status_details['Step']['Status']['State']
        
        if step_state in ['COMPLETED']:
            break
        elif step_state in ['CANCELLED', 'FAILED', 'INTERRUPTED']:
            raise Exception(f'Step {step_id} failed with state: {step_state}')
            
        time.sleep(30)
        
    return step_id

def terminate_emr_cluster(cluster_id):
    client = boto3.client('emr', region_name='eu-north-1')
    client.terminate_job_flows(JobFlowIds=[cluster_id])
    return cluster_id

def airflow_terminate_cluster(**context):
    ti = context['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    if cluster_id:
        terminate_emr_cluster(cluster_id)

# --- Orchestration logic with cluster creation/wait -
def airflow_create_cluster(**context):
    cluster_id = create_emr_cluster()
    wait_for_cluster(cluster_id)
    
    return cluster_id

def airflow_submit_emr_sensor_health(**context):
    ti = context['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    return submit_emr_job(
        kpi_script='kpi_sensor_health.py',
        job_name='SensorHealthKPIs',
        s3_bronze=os.environ['BRONZE_PATH'],
        iceberg_db=os.environ['ICEBERG_DB'],
        iceberg_tbl='sensor_health',
        iceberg_warehouse=f"s3://{os.environ['ICEBERG_BUCKET']}/",
        emr_cluster_id=cluster_id,
        emr_log_uri=os.environ['EMR_LOG_URI'],
        script_bucket=os.environ['SCRIPT_BUCKET']
    )

def airflow_submit_emr_temp_humidity(**context):
    ti = context['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    return submit_emr_job(
        kpi_script='kpi_temp_humidity.py',
        job_name='TempHumidityKPIs',
        s3_bronze=os.environ['BRONZE_PATH'],
        iceberg_db=os.environ['ICEBERG_DB'],
        iceberg_tbl='temp_humidity_kpis',
        iceberg_warehouse=f"s3://{os.environ['ICEBERG_BUCKET']}/",
        emr_cluster_id=cluster_id,
        emr_log_uri=os.environ['EMR_LOG_URI'],
        script_bucket=os.environ['SCRIPT_BUCKET']
    )

def airflow_submit_emr_event_geo(**context):
    ti = context['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr_cluster')
    return submit_emr_job(
        kpi_script='kpi_event_geo.py',
        job_name='EventGeoKPIs',
        s3_bronze=os.environ['BRONZE_PATH'],
        iceberg_db=os.environ['ICEBERG_DB'],
        iceberg_tbl='event_geo_kpis',
        iceberg_warehouse=f"s3://{os.environ['ICEBERG_BUCKET']}/",
        emr_cluster_id=cluster_id,
        emr_log_uri=os.environ['EMR_LOG_URI'],
        script_bucket=os.environ['SCRIPT_BUCKET']
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    'clickstream_emr_kpis',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)

create_cluster_task = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=airflow_create_cluster,
    dag=dag
)

kpi_sensor_health_task = PythonOperator(
    task_id='kpi_sensor_health',
    python_callable=airflow_submit_emr_sensor_health,
    dag=dag
)

kpi_temp_humidity_task = PythonOperator(
    task_id='kpi_temp_humidity',
    python_callable=airflow_submit_emr_temp_humidity,
    dag=dag
)

kpi_event_geo_task = PythonOperator(
    task_id='kpi_event_geo',
    python_callable=airflow_submit_emr_event_geo,
    dag=dag
)

terminate_cluster_task = PythonOperator(
    task_id='terminate_emr_cluster',
    python_callable=airflow_terminate_cluster,
    dag=dag,
    trigger_rule='all_done'
)

create_cluster_task >> kpi_sensor_health_task >> kpi_temp_humidity_task >> kpi_event_geo_task >> terminate_cluster_task
