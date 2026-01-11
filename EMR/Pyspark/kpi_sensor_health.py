import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Configuration
BRONZE_PATH = sys.argv[1] + "raw/"  # s3://bucket/clickstream/ + raw/ = s3://bucket/clickstream/raw/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'sensor_health'
ICEBERG_WAREHOUSE = sys.argv[4] if len(sys.argv) > 4 else "s3://your-iceberg-bucket/"  # e.g. 's3://bucket/'

# Init Spark Session with Iceberg support using Glue Catalog
# Note: We use a named catalog 'glue' instead of spark_catalog to avoid EMR default config conflicts
spark = SparkSession.builder \
    .appName("SensorHealthKPIs") \
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue.warehouse", ICEBERG_WAREHOUSE) \
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

logger.info("Spark session initialized")

# Read data from S3 bronze layer (recursively for partitioned Parquet data)
logger.info(f"Reading Parquet data from {BRONZE_PATH}")
clickstream = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_PATH)
logger.info("Data loaded successfully")

# Basic Sensor Health KPIs
status_counts = clickstream.groupBy("status").agg(count("sensor_id").alias("sensor_count"))

sensor_faulty_pct = clickstream.groupBy("sensor_id").agg(
    count(when(col("status") == "faulty", True)).alias("faulty_count"),
    count("*").alias("total_count")
).withColumn(
    "faulty_pct", col("faulty_count")/col("total_count")
)

logger.info("Data processed successfully")
logger.info(f"Sample sensor_faulty_pct data:\n{sensor_faulty_pct.show(5, truncate=False)}")

# Ensure Iceberg database & table exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue.{ICEBERG_DB}")
table_ddl = f"""
CREATE TABLE IF NOT EXISTS glue.{ICEBERG_DB}.{ICEBERG_TABLE} (
    sensor_id bigint,
    faulty_count bigint,
    total_count bigint,
    faulty_pct double
) USING iceberg
"""
spark.sql(table_ddl)

# Create status_health table for status aggregates
status_table_ddl = f"""
CREATE TABLE IF NOT EXISTS glue.{ICEBERG_DB}.status_health (
    status string,
    sensor_count bigint
) USING iceberg
"""
spark.sql(status_table_ddl)

# Save aggregated data to Iceberg tables
sensor_faulty_pct.select(
    "sensor_id", "faulty_count", "total_count", "faulty_pct"
).writeTo(f"glue.{ICEBERG_DB}.{ICEBERG_TABLE}").overwritePartitions()

status_counts.writeTo(f"glue.{ICEBERG_DB}.status_health").overwritePartitions()

logger.info("Data written successfully")

spark.stop()

