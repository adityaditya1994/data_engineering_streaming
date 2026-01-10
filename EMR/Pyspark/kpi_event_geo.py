import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format

BRONZE_PATH = sys.argv[1]  # s3://path/to/bronze/clickstream/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'event_geo_kpis'

spark = SparkSession.builder \
    .appName("EventGeoKPIs") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Read data from S3 bronze layer
clickstream = spark.read.json(BRONZE_PATH)

# Extract day
clickstream = clickstream.withColumn("day", date_format((col("timestamp")*1000).cast("timestamp"), "yyyy-MM-dd"))

# Event frequency by type and day
event_counts = clickstream.groupBy("day", "event_type").agg(count("*").alias("event_count"))

# Faulty sensors by geo location
faulty_sensors_by_geo = clickstream.where(col("status") == "faulty") \
    .groupBy("location.latitude", "location.longitude") \
    .agg(count("sensor_id").alias("faulty_count"))

# Create Iceberg table if needed
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DB}")

# Event counts table
event_table_ddl = f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_TABLE}_events (
    day string,
    event_type string,
    event_count bigint
) USING iceberg
"""
spark.sql(event_table_ddl)
event_counts.writeTo(f"{ICEBERG_DB}.{ICEBERG_TABLE}_events").overwritePartitions()

# Faulty sensors by location table
geo_table_ddl = f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_TABLE}_faulty_geo (
    latitude double,
    longitude double,
    faulty_count bigint
) USING iceberg
"""
spark.sql(geo_table_ddl)
faulty_sensors_by_geo.writeTo(f"{ICEBERG_DB}.{ICEBERG_TABLE}_faulty_geo").overwritePartitions()

spark.stop()

