import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, date_format

BRONZE_PATH = sys.argv[1]  # s3://path/to/bronze/clickstream/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'temp_humidity_kpis'

spark = SparkSession.builder \
    .appName("TempHumidityKPIs") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Read data from S3 bronze layer
clickstream = spark.read.json(BRONZE_PATH)

# Generate a day column for time-based grouping
clickstream = clickstream.withColumn("day", date_format((col("timestamp")*1000).cast("timestamp"), "yyyy-MM-dd"))

# Temperature & humidity aggregates by day and location
metrics = clickstream.groupBy("day", "location.latitude", "location.longitude").agg(
    avg("temperature").alias("avg_temp"),
    min("temperature").alias("min_temp"),
    max("temperature").alias("max_temp"),
    avg("humidity").alias("avg_humidity"),
    min("humidity").alias("min_humidity"),
    max("humidity").alias("max_humidity")
)

# Create Iceberg table if needed
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DB}")
table_ddl = f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_DB}.{ICEBERG_TABLE} (
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
spark.sql(table_ddl)

metrics.writeTo(f"{ICEBERG_DB}.{ICEBERG_TABLE}").overwritePartitions()
spark.stop()

