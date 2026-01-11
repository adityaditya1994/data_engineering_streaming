import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format

BRONZE_PATH = sys.argv[1] + "raw/"  # s3://bucket/clickstream/ + raw/ = s3://bucket/clickstream/raw/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'event_geo_kpis'
ICEBERG_WAREHOUSE = sys.argv[4] if len(sys.argv) > 4 else "s3://your-iceberg-bucket/"  # e.g. 's3://bucket/'

# Init Spark Session with Iceberg support using Glue Catalog
# Note: We use a named catalog 'glue' instead of spark_catalog to avoid EMR default config conflicts
spark = SparkSession.builder \
    .appName("EventGeoKPIs") \
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

# Extract day
clickstream = clickstream.withColumn("day", date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd"))

# Event frequency by type and day
event_counts = clickstream.groupBy("day", "event_type").agg(count("*").alias("event_count"))

# Faulty sensors by geo location
faulty_sensors_by_geo = clickstream.where(col("status") == "faulty") \
    .groupBy("location.latitude", "location.longitude") \
    .agg(count("sensor_id").alias("faulty_count"))

# Create Iceberg table if needed
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue.{ICEBERG_DB}")

# Event counts table
event_table_ddl = f"""
CREATE TABLE IF NOT EXISTS glue.{ICEBERG_DB}.{ICEBERG_TABLE}_events (
    day string,
    event_type string,
    event_count bigint
) USING iceberg
"""
spark.sql(event_table_ddl)
event_counts.writeTo(f"glue.{ICEBERG_DB}.{ICEBERG_TABLE}_events").overwritePartitions()

# Faulty sensors by location table
geo_table_ddl = f"""
CREATE TABLE IF NOT EXISTS glue.{ICEBERG_DB}.{ICEBERG_TABLE}_faulty_geo (
    latitude double,
    longitude double,
    faulty_count bigint
) USING iceberg
"""
spark.sql(geo_table_ddl)
faulty_sensors_by_geo.writeTo(f"glue.{ICEBERG_DB}.{ICEBERG_TABLE}_faulty_geo").overwritePartitions()

logger.info("Data written successfully")

spark.stop()

