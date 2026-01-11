"""
Iceberg Table Creation for Local Spark (Jupyter/Dev Environment)
Mirrors the EMR production scripts but runs locally

IMPORTANT: Local Spark typically cannot connect to AWS Glue Metastore 
(it requires special JARs and VPC configs). This script uses a LOCAL 
Iceberg catalog with data stored in S3 as an alternative.

For production, use the EMR scripts which connect to Glue directly.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, min, max, date_format

# --- Configuration ---
# Set these environment variables or update defaults for your AWS account
BRONZE_BUCKET = os.environ.get('BRONZE_PATH', 's3a://your-bronze-bucket/clickstream/')
ICEBERG_BUCKET = os.environ.get('ICEBERG_BUCKET', 'your-iceberg-bucket')
ICEBERG_DB = "analytics"

# S3 paths
S3_INPUT_PATH = BRONZE_BUCKET
S3_ICEBERG_WAREHOUSE = f"s3a://{ICEBERG_BUCKET}/local_warehouse"

print(f"🔹 Input: {S3_INPUT_PATH}")
print(f"🔸 Iceberg Warehouse: {S3_ICEBERG_WAREHOUSE}")

# --- Spark Session with Iceberg ---
def create_iceberg_spark_session():
    """
    Creates a Spark session configured for Apache Iceberg with S3.
    Uses a local Hadoop catalog (not Glue) for dev/local execution.
    """
    builder = SparkSession.builder \
        .appName("LocalIcebergKPIs") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", S3_ICEBERG_WAREHOUSE) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # AWS Credentials from environment
    if os.environ.get('AWS_ACCESS_KEY_ID'):
        print("✅ Using credentials from Environment Variables")
        builder = builder \
            .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    else:
        print("⚠️ Using default AWS credentials provider")
        builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                                 "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    return builder.getOrCreate()


spark = create_iceberg_spark_session()
print("✅ Spark session initialized with Iceberg support")

# --- Read Data ---
print(f"📖 Reading data from {S3_INPUT_PATH}")
df = spark.read.json(S3_INPUT_PATH)
print(f"✅ Loaded {df.count()} records")
df.printSchema()

# --- Create Iceberg Database ---
print(f"🗄️ Creating database: local.{ICEBERG_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{ICEBERG_DB}")
print("✅ Database created or already exists")

# --- KPI 1: Sensor Health ---
print("📊 Calculating Sensor Health KPIs...")
sensor_faulty_pct = df.groupBy("sensor_id").agg(
    count(when(col("status") == "faulty", True)).alias("faulty_count"),
    count("*").alias("total_count")
).withColumn("faulty_pct", col("faulty_count")/col("total_count"))

# Create Iceberg table
sensor_table = f"local.{ICEBERG_DB}.sensor_health"
print(f"🧊 Creating Iceberg table: {sensor_table}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {sensor_table} (
    sensor_id bigint,
    faulty_count bigint,
    total_count bigint,
    faulty_pct double
) USING iceberg
""")

# Write data
print(f"💾 Writing data to {sensor_table}")
sensor_faulty_pct.writeTo(sensor_table).overwritePartitions()
print("✅ Sensor Health KPIs written")

# --- KPI 2: Temp/Humidity ---
print("📊 Calculating Temp/Humidity KPIs...")
df_day = df.withColumn("day", date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd"))
temp_humidity = df_day.groupBy("day", "location.latitude", "location.longitude").agg(
    avg("temperature").alias("avg_temp"),
    min("temperature").alias("min_temp"),
    max("temperature").alias("max_temp"),
    avg("humidity").alias("avg_humidity"),
    min("humidity").alias("min_humidity"),
    max("humidity").alias("max_humidity")
)

temp_table = f"local.{ICEBERG_DB}.temp_humidity_kpis"
print(f"🧊 Creating Iceberg table: {temp_table}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {temp_table} (
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
""")

print(f"💾 Writing data to {temp_table}")
temp_humidity.writeTo(temp_table).overwritePartitions()
print("✅ Temp/Humidity KPIs written")

# --- Verify ---
print("\n📋 Listing tables in local.analytics:")
spark.sql(f"SHOW TABLES IN local.{ICEBERG_DB}").show()

print("\n📋 Sample data from sensor_health:")
spark.sql(f"SELECT * FROM {sensor_table} LIMIT 5").show()

print("\n🎉 Done! Check your S3 bucket for Iceberg data at:")
print(f"   {S3_ICEBERG_WAREHOUSE}/{ICEBERG_DB}/")

spark.stop()
