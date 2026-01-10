from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import time
import json

def debug_log(hypothesisId, location, message, data=None):
    # region agent log
    debug_payload = {
        'sessionId': 'debug-session',
        'runId': 'initial',
        'hypothesisId': hypothesisId,
        'location': location,
        'message': message,
        'data': data,
        'timestamp': int(time.time() * 1000)
    }
    with open("/Users/adityasinghal/Documents/GitHub/coderepo/airflow_emr/.cursor/debug.log", "a") as f:
        f.write(json.dumps(debug_payload) + "\n")
    # endregion

def create_emr_cluster():
    # region agent log
    debug_log('H1', 'clickstream_emr_kpis_dag.py:create_emr_cluster:entry', 'Entered create_emr_cluster')
    # endregion
    client = boto3.client('emr', region_name='eu-north-1')
    # Cheapest config: 1 master + 2 core, both m5.xlarge as minimal for Iceberg + Glue
    response = client.run_job_flow(
        Name='kpi-teaching-emr',
        LogUri=os.environ['EMR_LOG_URI'],
        ReleaseLabel='emr-6.15.0',
        Applications=[{'Name': 'Spark'}],
        Instances={
            'InstanceGroups': [
                {'Name': 'Master nodes', 'Market': 'ON_DEMAND', 'InstanceRole': 'MASTER', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
                {'Name': 'Core nodes', 'Market': 'ON_DEMAND', 'InstanceRole': 'CORE', 'InstanceType': 'm5.xlarge', 'InstanceCount': 2}
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        JobFlowRole=os.environ.get('EMR_EC2_ROLE', 'EMR_EC2_DefaultRole'),
        ServiceRole=os.environ.get('EMR_ROLE', 'EMR_DefaultRole'),
        VisibleToAllUsers=True
    )
    cluster_id = response['JobFlowId']
    debug_log('H1', 'clickstream_emr_kpis_dag.py:create_emr_cluster:exit', 'Created cluster', {'cluster_id': cluster_id, 'conf': response})
    return cluster_id

def wait_for_cluster(cluster_id):
    client = boto3.client('emr', region_name='eu-north-1')
    state = None
    while state not in ('WAITING', 'RUNNING'):
        time.sleep(30)
        desc = client.describe_cluster(ClusterId=cluster_id)
        state = desc['Cluster']['Status']['State']
        debug_log('H2', 'clickstream_emr_kpis_dag.py:wait_for_cluster', f'Waiting for cluster state: {state}', {'cluster_id': cluster_id})
    debug_log('H2', 'clickstream_emr_kpis_dag.py:wait_for_cluster', f'Cluster ready: {state}', {'cluster_id': cluster_id})
    return state

def submit_emr_job(kpi_script, job_name, s3_bronze, iceberg_db, iceberg_tbl, emr_cluster_id, emr_log_uri, script_bucket, **context):
    debug_log('H4', 'clickstream_emr_kpis_dag.py:submit_emr_job:entry', 'Submitting EMR job', {
        'kpi_script': kpi_script, 'cluster_id': emr_cluster_id, 'job_name': job_name
    })
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
                iceberg_tbl
            ]
        }
    }
    response = client.add_job_flow_steps(
        JobFlowId=emr_cluster_id,
        Steps=[step]
    )
    step_id = response['StepIds'][0]
    debug_log('H4', 'clickstream_emr_kpis_dag.py:submit_emr_job:exit', f'Submitted {job_name}', {'step_id': step_id})
    return step_id

# --- Orchestration logic with cluster creation/wait -
def airflow_create_cluster(**context):
    cluster_id = create_emr_cluster()
    wait_for_cluster(cluster_id)
    debug_log('H3', 'clickstream_emr_kpis_dag.py:airflow_create_cluster', 'Cluster up and ready', {'cluster_id': cluster_id})
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

create_cluster_task >> kpi_sensor_health_task >> kpi_temp_humidity_task >> kpi_event_geo_task
