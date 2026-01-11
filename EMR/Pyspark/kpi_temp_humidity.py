import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, date_format

BRONZE_PATH = sys.argv[1] + "raw/"  # s3://bucket/clickstream/ + raw/ = s3://bucket/clickstream/raw/
ICEBERG_DB = sys.argv[2]   # e.g. 'analytics'
ICEBERG_TABLE = sys.argv[3] # e.g. 'temp_humidity_kpis'
ICEBERG_WAREHOUSE = sys.argv[4] if len(sys.argv) > 4 else "s3://your-iceberg-bucket/"  # e.g. 's3://bucket/'

# Init Spark Session with Iceberg support using Glue Catalog
# Note: We use a named catalog 'glue' instead of spark_catalog to avoid EMR default config conflicts
spark = SparkSession.builder \
    .appName("TempHumidityKPIs") \
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

# Generate a day column for time-based grouping
clickstream = clickstream.withColumn("day", date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd"))

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
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue.{ICEBERG_DB}")
table_ddl = f"""
CREATE TABLE IF NOT EXISTS glue.{ICEBERG_DB}.{ICEBERG_TABLE} (
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

metrics.writeTo(f"glue.{ICEBERG_DB}.{ICEBERG_TABLE}").overwritePartitions()

logger.info("Data written successfully")
spark.stop()

