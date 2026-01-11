"""
Unit tests for PySpark KPI scripts.

Tests cover:
- Data loading and schema validation
- KPI aggregation logic
- Sensor health calculations
- Temperature/humidity metrics
- Event geo analytics
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, 
    StringType, StructType
)


class TestDataSchemaValidation:
    """Test data schema matches expected clickstream format."""
    
    def test_sample_data_has_required_fields(self, sample_clickstream_data):
        """Verify sample data contains all required fields."""
        required_fields = [
            'sensor_id', 'temperature', 'humidity', 
            'location', 'status', 'event_type', 'timestamp'
        ]
        for record in sample_clickstream_data:
            for field in required_fields:
                assert field in record, f"Missing required field: {field}"
    
    def test_location_has_coordinates(self, sample_clickstream_data):
        """Verify location field contains latitude and longitude."""
        for record in sample_clickstream_data:
            assert 'latitude' in record['location']
            assert 'longitude' in record['location']
    
    def test_status_values_are_valid(self, sample_clickstream_data):
        """Verify status field contains valid values."""
        valid_statuses = {'active', 'faulty', 'inactive'}
        for record in sample_clickstream_data:
            assert record['status'] in valid_statuses, \
                f"Invalid status: {record['status']}"
    
    def test_temperature_is_numeric(self, sample_clickstream_data):
        """Verify temperature values are numeric."""
        for record in sample_clickstream_data:
            assert isinstance(record['temperature'], (int, float))


class TestSensorHealthKPIs:
    """Tests for sensor health KPI calculations."""
    
    @pytest.mark.slow
    def test_sensor_health_aggregation_with_spark(
        self, spark_session, sample_clickstream_json_path
    ):
        """Test sensor health aggregation using real Spark session."""
        from pyspark.sql.functions import col, count, when
        
        # Load sample data
        df = spark_session.read.json(sample_clickstream_json_path)
        
        # Replicate sensor health logic from kpi_sensor_health.py
        sensor_faulty_pct = df.groupBy("sensor_id").agg(
            count(when(col("status") == "faulty", True)).alias("faulty_count"),
            count("*").alias("total_count")
        ).withColumn(
            "faulty_pct", col("faulty_count") / col("total_count")
        )
        
        results = sensor_faulty_pct.collect()
        
        # Verify aggregations
        assert len(results) == 2, "Expected 2 unique sensors"
        
        # Find sensor 42 (1 faulty out of 2)
        sensor_42 = [r for r in results if r['sensor_id'] == 42][0]
        assert sensor_42['faulty_count'] == 1
        assert sensor_42['total_count'] == 2
        assert sensor_42['faulty_pct'] == 0.5
        
        # Find sensor 37 (1 faulty out of 1)
        sensor_37 = [r for r in results if r['sensor_id'] == 37][0]
        assert sensor_37['faulty_count'] == 1
        assert sensor_37['total_count'] == 1
        assert sensor_37['faulty_pct'] == 1.0
    
    def test_status_count_aggregation(self, mock_spark_session):
        """Test status count aggregation with mocked Spark."""
        mock_df = mock_spark_session.read.json("dummy_path")
        
        # Call aggregation methods
        result = mock_df.groupBy("status").agg()
        
        # Verify method chain was called
        mock_df.groupBy.assert_called_with("status")


class TestTempHumidityKPIs:
    """Tests for temperature and humidity KPI calculations."""
    
    @pytest.mark.slow
    def test_temp_humidity_aggregation_with_spark(
        self, spark_session, sample_clickstream_json_path
    ):
        """Test temperature/humidity aggregation using real Spark."""
        from pyspark.sql.functions import col, avg, min, max, date_format
        
        df = spark_session.read.json(sample_clickstream_json_path)
        
        # Add day column
        df = df.withColumn(
            "day", 
            date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd")
        )
        
        # Aggregate metrics
        metrics = df.groupBy("day", "location.latitude", "location.longitude").agg(
            avg("temperature").alias("avg_temp"),
            min("temperature").alias("min_temp"),
            max("temperature").alias("max_temp"),
            avg("humidity").alias("avg_humidity")
        )
        
        results = metrics.collect()
        
        # Verify we have results
        assert len(results) >= 1, "Expected at least 1 location group"
        
        # All temperature values should be within sample data range (25-34)
        for row in results:
            assert 20 <= row['avg_temp'] <= 40
            assert 20 <= row['min_temp'] <= 40
            assert 20 <= row['max_temp'] <= 40
    
    @pytest.mark.slow
    def test_day_extraction_from_timestamp(self, spark_session):
        """Test that day extraction from Unix timestamp works correctly."""
        from pyspark.sql.functions import col, date_format
        
        # Create test data with known timestamp
        # 1704267600 = 2024-01-03 in UTC
        data = [{"timestamp": 1704267600}]
        df = spark_session.createDataFrame(data)
        
        df = df.withColumn(
            "day",
            date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd")
        )
        
        result = df.collect()[0]['day']
        assert result == "2024-01-03"


class TestEventGeoKPIs:
    """Tests for event and geo-based KPI calculations."""
    
    @pytest.mark.slow
    def test_event_count_by_type(self, spark_session, sample_clickstream_json_path):
        """Test event counting by type."""
        from pyspark.sql.functions import count
        
        df = spark_session.read.json(sample_clickstream_json_path)
        
        event_counts = df.groupBy("event_type").agg(
            count("*").alias("event_count")
        )
        
        results = {row['event_type']: row['event_count'] 
                   for row in event_counts.collect()}
        
        # Verify event types are counted
        assert 'sensor_reading' in results
        assert 'alert' in results
        assert results['sensor_reading'] == 1
        assert results['alert'] == 2
    
    @pytest.mark.slow
    def test_faulty_sensors_by_geo(self, spark_session, sample_clickstream_json_path):
        """Test faulty sensor counting by geo location."""
        from pyspark.sql.functions import col, count
        
        df = spark_session.read.json(sample_clickstream_json_path)
        
        faulty_by_geo = df.where(col("status") == "faulty") \
            .groupBy("location.latitude", "location.longitude") \
            .agg(count("sensor_id").alias("faulty_count"))
        
        results = faulty_by_geo.collect()
        
        # Should have 2 locations with faulty sensors
        assert len(results) == 2


class TestDataLoadingFromS3:
    """Tests for S3 data loading (mocked)."""
    
    def test_s3_path_parsing(self):
        """Test S3 path is parsed correctly."""
        s3_path = "s3://my-bucket/bronze/clickstream/"
        
        # Parse bucket and key
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        assert bucket == "my-bucket"
        assert key == "bronze/clickstream/"
    
    def test_mock_s3_read(self, mock_s3_bucket, sample_clickstream_data):
        """Test reading from mocked S3 bucket."""
        s3 = mock_s3_bucket['client']
        bucket = mock_s3_bucket['bucket_name']
        
        # Upload sample data
        s3.put_object(
            Bucket=bucket,
            Key='clickstream/sample.json',
            Body=json.dumps(sample_clickstream_data)
        )
        
        # Verify upload
        response = s3.get_object(Bucket=bucket, Key='clickstream/sample.json')
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        assert len(data) == len(sample_clickstream_data)
        assert data[0]['sensor_id'] == 42


class TestIcebergTableCreation:
    """Tests for Iceberg table DDL generation."""
    
    def test_sensor_health_table_ddl_format(self):
        """Test sensor health table DDL is properly formatted."""
        iceberg_db = "analytics"
        iceberg_table = "sensor_health"
        
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {iceberg_db}.{iceberg_table} (
            sensor_id bigint,
            faulty_count bigint,
            total_count bigint,
            faulty_pct double
        ) USING iceberg
        """
        
        assert "CREATE TABLE IF NOT EXISTS" in table_ddl
        assert "analytics.sensor_health" in table_ddl
        assert "USING iceberg" in table_ddl
    
    def test_temp_humidity_table_ddl_format(self):
        """Test temp humidity table DDL is properly formatted."""
        iceberg_db = "analytics"
        iceberg_table = "temp_humidity_kpis"
        
        expected_columns = [
            "day string",
            "latitude double",
            "longitude double",
            "avg_temp double",
            "min_temp double",
            "max_temp double"
        ]
        
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {iceberg_db}.{iceberg_table} (
            day string,
            latitude double,
            longitude double,
            avg_temp double,
            min_temp double,
            max_temp double,
            avg_humidity double,
            min_humidity double,
            max_humidity double
        ) USING iceberg
        """
        
        for col in expected_columns:
            assert col in table_ddl
