"""
Unit tests for Airflow DAG validation.

Tests cover:
- DAG loading without errors
- Task dependencies
- Required environment variables
- EMR submission logic
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add airflow directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow'))


class TestDAGLoading:
    """Tests for DAG file loading and parsing."""
    
    def test_dag_import_without_errors(self, airflow_env_vars, airflow_home):
        """Test that DAG file can be imported without errors."""
        # Patch the debug_log to avoid file writing issues
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            assert dag is not None
            assert dag.dag_id == 'clickstream_emr_kpis'
    
    def test_dag_has_expected_tasks(self, airflow_env_vars, airflow_home):
        """Test that DAG contains all expected tasks."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            expected_tasks = [
                'create_emr_cluster',
                'kpi_sensor_health',
                'kpi_temp_humidity',
                'kpi_event_geo'
            ]
            
            task_ids = [task.task_id for task in dag.tasks]
            
            for expected in expected_tasks:
                assert expected in task_ids, f"Missing task: {expected}"
    
    def test_dag_task_count(self, airflow_env_vars, airflow_home):
        """Test DAG has correct number of tasks."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            assert len(dag.tasks) == 4


class TestTaskDependencies:
    """Tests for task dependency chain."""
    
    def test_kpi_tasks_depend_on_cluster_creation(self, airflow_env_vars, airflow_home):
        """Test that KPI tasks depend on cluster creation."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            # Get task objects
            tasks = {task.task_id: task for task in dag.tasks}
            
            create_cluster = tasks['create_emr_cluster']
            kpi_sensor = tasks['kpi_sensor_health']
            
            # Check dependency chain
            downstream_ids = [t.task_id for t in create_cluster.downstream_list]
            assert 'kpi_sensor_health' in downstream_ids
    
    def test_sequential_kpi_execution(self, airflow_env_vars, airflow_home):
        """Test that KPI tasks run sequentially."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            tasks = {task.task_id: task for task in dag.tasks}
            
            # sensor_health -> temp_humidity -> event_geo
            sensor_downstream = [t.task_id for t in tasks['kpi_sensor_health'].downstream_list]
            assert 'kpi_temp_humidity' in sensor_downstream
            
            temp_downstream = [t.task_id for t in tasks['kpi_temp_humidity'].downstream_list]
            assert 'kpi_event_geo' in temp_downstream


class TestDAGConfiguration:
    """Tests for DAG configuration settings."""
    
    def test_dag_schedule_is_none(self, airflow_env_vars, airflow_home):
        """Test DAG is manually triggered (no schedule)."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            assert dag.schedule is None
    
    def test_dag_catchup_disabled(self, airflow_env_vars, airflow_home):
        """Test DAG catchup is disabled."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            assert dag.catchup is False
    
    def test_dag_default_args(self, airflow_env_vars, airflow_home):
        """Test DAG default arguments are set correctly."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            from clickstream_emr_kpis_dag import dag
            
            assert dag.default_args['owner'] == 'airflow'
            assert dag.default_args['depends_on_past'] is False
            assert dag.default_args['retries'] == 1


class TestEMRSubmission:
    """Tests for EMR job submission logic."""
    
    def test_submit_emr_job_builds_correct_step(self, airflow_env_vars):
        """Test that submit_emr_job builds correct step configuration."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            with patch('clickstream_emr_kpis_dag.boto3') as mock_boto3:
                mock_client = MagicMock()
                mock_boto3.client.return_value = mock_client
                mock_client.add_job_flow_steps.return_value = {'StepIds': ['s-12345']}
                
                from clickstream_emr_kpis_dag import submit_emr_job
                
                step_id = submit_emr_job(
                    kpi_script='kpi_sensor_health.py',
                    job_name='TestJob',
                    s3_bronze='s3://bronze/data/',
                    iceberg_db='analytics',
                    iceberg_tbl='sensor_health',
                    emr_cluster_id='j-CLUSTER123',
                    emr_log_uri='s3://logs/',
                    script_bucket='scripts-bucket'
                )
                
                # Verify step was submitted
                mock_client.add_job_flow_steps.assert_called_once()
                call_args = mock_client.add_job_flow_steps.call_args
                
                assert call_args[1]['JobFlowId'] == 'j-CLUSTER123'
                assert step_id == 's-12345'
    
    def test_create_emr_cluster_calls_run_job_flow(self, airflow_env_vars):
        """Test that create_emr_cluster calls run_job_flow correctly."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            with patch('clickstream_emr_kpis_dag.boto3') as mock_boto3:
                mock_client = MagicMock()
                mock_boto3.client.return_value = mock_client
                mock_client.run_job_flow.return_value = {'JobFlowId': 'j-NEWCLUSTER'}
                
                from clickstream_emr_kpis_dag import create_emr_cluster
                
                cluster_id = create_emr_cluster()
                
                # Verify cluster creation was called
                mock_client.run_job_flow.assert_called_once()
                assert cluster_id == 'j-NEWCLUSTER'


class TestEnvironmentVariables:
    """Tests for required environment variable handling."""
    
    def test_required_env_vars_are_documented(self):
        """Test that required environment variables are documented."""
        required_vars = [
            'EMR_CLUSTER_ID',
            'SCRIPT_BUCKET', 
            'BRONZE_PATH',
            'ICEBERG_DB',
            'EMR_LOG_URI'
        ]
        
        # These should be documented in the README
        readme_path = os.path.join(
            os.path.dirname(__file__), '..', 'airflow', 'README.md'
        )
        
        with open(readme_path, 'r') as f:
            readme_content = f.read()
        
        for var in required_vars:
            assert var in readme_content, f"Missing documentation for {var}"
    
    def test_env_vars_used_in_dag(self, airflow_env_vars, airflow_home):
        """Test that environment variables are accessed in DAG."""
        with patch('clickstream_emr_kpis_dag.debug_log'):
            # Just verify DAG loads with env vars set
            from clickstream_emr_kpis_dag import dag
            assert dag is not None


class TestEMRBotoHelper:
    """Tests for emr_boto_helper module."""
    
    def test_submit_emr_spark_step_returns_step_id(self):
        """Test that submit_emr_spark_step returns step ID."""
        with patch('emr_boto_helper.boto3') as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client
            mock_client.add_job_flow_steps.return_value = {'StepIds': ['s-HELPER123']}
            
            from emr_boto_helper import submit_emr_spark_step
            
            step_id = submit_emr_spark_step(
                emr_cluster_id='j-CLUSTER',
                script_s3_path='s3://bucket/script.py',
                input_path='s3://bronze/input/',
                iceberg_db='analytics',
                iceberg_tbl='table'
            )
            
            assert step_id == 's-HELPER123'
    
    def test_submit_step_uses_command_runner(self):
        """Test that step uses command-runner.jar."""
        with patch('emr_boto_helper.boto3') as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.client.return_value = mock_client
            mock_client.add_job_flow_steps.return_value = {'StepIds': ['s-123']}
            
            from emr_boto_helper import submit_emr_spark_step
            
            submit_emr_spark_step(
                emr_cluster_id='j-CLUSTER',
                script_s3_path='s3://bucket/script.py',
                input_path='s3://bronze/',
                iceberg_db='db',
                iceberg_tbl='tbl'
            )
            
            call_args = mock_client.add_job_flow_steps.call_args
            step = call_args[1]['Steps'][0]
            
            assert step['HadoopJarStep']['Jar'] == 'command-runner.jar'
            assert 'spark-submit' in step['HadoopJarStep']['Args']
