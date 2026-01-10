"""
Pytest conftest.py - Shared fixtures for Airflow EMR project tests.

Provides:
- Mock Spark session for PySpark tests
- Mock S3 environment using moto
- Sample data fixtures
- Airflow testing utilities
"""
import os
import sys
import json
import pytest
from unittest.mock import MagicMock, patch

# Add project paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'EMR', 'Pyspark'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow'))


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_clickstream_data():
    """Sample clickstream data matching sample_clickstream.json format."""
    return [
        {
            "sensor_id": 42,
            "temperature": 25.4,
            "humidity": 62.1,
            "location": {"latitude": 22.5, "longitude": 88.3},
            "status": "active",
            "event_type": "sensor_reading",
            "timestamp": 1704267600
        },
        {
            "sensor_id": 37,
            "temperature": 33.1,
            "humidity": 55.7,
            "location": {"latitude": -10.2, "longitude": 120.8},
            "status": "faulty",
            "event_type": "alert",
            "timestamp": 1704267660
        },
        {
            "sensor_id": 42,
            "temperature": 26.0,
            "humidity": 60.0,
            "location": {"latitude": 22.5, "longitude": 88.3},
            "status": "faulty",
            "event_type": "alert",
            "timestamp": 1704267720
        }
    ]


@pytest.fixture
def sample_clickstream_json_path(tmp_path, sample_clickstream_data):
    """Create a temporary JSON file with sample data."""
    json_file = tmp_path / "sample_clickstream.json"
    json_file.write_text(json.dumps(sample_clickstream_data))
    return str(json_file)


# ============================================================================
# Spark Session Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local Spark session for testing.
    Uses 'local[*]' master for parallel processing on available cores.
    
    Note: This fixture is session-scoped to avoid repeated Spark initialization.
    """
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestSession") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not installed - skipping Spark tests")


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for unit tests that don't need real Spark."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.read.json.return_value = mock_df
    mock_spark.sql.return_value = mock_df
    mock_df.groupBy.return_value = mock_df
    mock_df.agg.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.where.return_value = mock_df
    mock_df.writeTo.return_value = mock_df
    mock_df.overwritePartitions.return_value = None
    return mock_spark


# ============================================================================
# AWS Mock Fixtures (using moto)
# ============================================================================

@pytest.fixture
def mock_aws_credentials():
    """Set mock AWS credentials for testing."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-north-1'
    yield
    # Cleanup
    for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 
                'AWS_SECURITY_TOKEN', 'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION']:
        os.environ.pop(key, None)


@pytest.fixture
def mock_s3_bucket(mock_aws_credentials):
    """Create mock S3 bucket using moto."""
    try:
        import boto3
        from moto import mock_aws
        
        with mock_aws():
            s3 = boto3.client('s3', region_name='eu-north-1')
            bucket_name = 'test-bronze-bucket'
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
            )
            yield {'client': s3, 'bucket_name': bucket_name}
    except ImportError:
        pytest.skip("moto not installed - skipping S3 tests")


@pytest.fixture
def mock_emr_client(mock_aws_credentials):
    """Create mock EMR client using moto."""
    try:
        import boto3
        from moto import mock_aws
        
        with mock_aws():
            emr = boto3.client('emr', region_name='eu-north-1')
            yield emr
    except ImportError:
        pytest.skip("moto not installed - skipping EMR tests")


# ============================================================================
# Airflow Environment Fixtures
# ============================================================================

@pytest.fixture
def airflow_env_vars():
    """Set required Airflow environment variables for DAG testing."""
    env_vars = {
        'EMR_CLUSTER_ID': 'j-TESTCLUSTER123',
        'SCRIPT_BUCKET': 'test-scripts-bucket',
        'BRONZE_PATH': 's3://test-bronze-bucket/clickstream/',
        'ICEBERG_DB': 'test_analytics',
        'EMR_LOG_URI': 's3://test-logs-bucket/emr-logs/',
        'EMR_EC2_ROLE': 'EMR_EC2_TestRole',
        'EMR_ROLE': 'EMR_TestRole'
    }
    for key, value in env_vars.items():
        os.environ[key] = value
    yield env_vars
    for key in env_vars:
        os.environ.pop(key, None)


@pytest.fixture
def airflow_home(tmp_path):
    """Create temporary Airflow home for testing."""
    airflow_dir = tmp_path / "airflow"
    airflow_dir.mkdir()
    os.environ['AIRFLOW_HOME'] = str(airflow_dir)
    yield str(airflow_dir)
    os.environ.pop('AIRFLOW_HOME', None)
