import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Configuration
BRONZE_PATH = sys.argv[1]  # s3://path/to/bronze/clickstream/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'sensor_health'

# Init Spark Session with Iceberg support
spark = SparkSession.builder \
    .appName("SensorHealthKPIs") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Read data from S3 bronze layer
clickstream = spark.read.json(BRONZE_PATH)

# Basic Sensor Health KPIs
status_counts = clickstream.groupBy("status").agg(count("sensor_id").alias("sensor_count"))

sensor_faulty_pct = clickstream.groupBy("sensor_id").agg(
    count(when(col("status") == "faulty", True)).alias("faulty_count"),
    count("*").alias("total_count")
).withColumn(
    "faulty_pct", col("faulty_count")/col("total_count")
)

# Ensure Iceberg database & table exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DB}")
table_ddl = f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_TABLE} (
    sensor_id bigint,
    faulty_count bigint,
    total_count bigint,
    faulty_pct double
) USING iceberg
"""
spark.sql(table_ddl)

# Save aggregated data to Iceberg table
sensor_faulty_pct.select(
    "sensor_id", "faulty_count", "total_count", "faulty_pct"
).writeTo(f"{ICEBERG_DB}.{ICEBERG_TABLE}").overwritePartitions()

status_counts.writeTo(f"{ICEBERG_DB}.status_health").overwritePartitions()

spark.stop()

