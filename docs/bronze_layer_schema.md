# Bronze Layer Table Schema

This document defines the recommended **bronze layer** table schema for the clickstream data ingestion pipeline.

## Overview

The bronze layer represents **raw, unprocessed data** as it lands from source systems. This schema is based on analysis of `sample_clickstream.json` and designed for:
- IoT sensor data ingestion
- Event tracking and alerting
- Geolocation-based analytics

---

## Table: `bronze.clickstream_raw`

| Field Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `sensor_id` | BIGINT | No | Unique identifier for the IoT sensor |
| `temperature` | DOUBLE | Yes | Temperature reading in Celsius |
| `humidity` | DOUBLE | Yes | Relative humidity percentage (0-100) |
| `location` | STRUCT | No | Geographic coordinates (see nested fields below) |
| `location.latitude` | DOUBLE | No | GPS latitude (-90 to 90) |
| `location.longitude` | DOUBLE | No | GPS longitude (-180 to 180) |
| `status` | STRING | No | Sensor operational status. Enum: `active`, `faulty`, `inactive` |
| `event_type` | STRING | No | Event classification. Enum: `sensor_reading`, `alert`, `heartbeat` |
| `timestamp` | BIGINT | No | Unix epoch timestamp (seconds since 1970-01-01) |
| `_ingestion_time` | TIMESTAMP | No | System timestamp when record was ingested (auto-populated) |
| `_source_file` | STRING | Yes | S3 path of source file (for lineage tracking) |
| `_batch_id` | STRING | Yes | Ingestion batch identifier (for reprocessing) |

---

## Sample Data

```json
{
  "sensor_id": 42,
  "temperature": 25.4,
  "humidity": 62.1,
  "location": {"latitude": 22.5, "longitude": 88.3},
  "status": "active",
  "event_type": "sensor_reading",
  "timestamp": 1704267600
}
```

---

## Spark Schema Definition

```python
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, 
    StringType, TimestampType
)

bronze_schema = StructType([
    StructField("sensor_id", LongType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False)
    ]), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("_ingestion_time", TimestampType(), nullable=False),
    StructField("_source_file", StringType(), nullable=True),
    StructField("_batch_id", StringType(), nullable=True)
])
```

---

## Iceberg Table DDL

```sql
CREATE TABLE IF NOT EXISTS bronze.clickstream_raw (
    sensor_id BIGINT NOT NULL,
    temperature DOUBLE,
    humidity DOUBLE,
    location STRUCT<latitude: DOUBLE, longitude: DOUBLE> NOT NULL,
    status STRING NOT NULL,
    event_type STRING NOT NULL,
    timestamp BIGINT NOT NULL,
    _ingestion_time TIMESTAMP NOT NULL,
    _source_file STRING,
    _batch_id STRING
)
USING iceberg
PARTITIONED BY (days(from_unixtime(timestamp)))
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.compression-codec' = 'gzip'
);
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **`timestamp` as BIGINT** | Stored as Unix epoch for source fidelity; convert to TIMESTAMP in silver layer |
| **Nested `location` struct** | Preserves original JSON structure; flattened in KPI aggregations |
| **Added `_ingestion_*` fields** | Standard metadata for data lineage and debugging |
| **Partition by day** | Optimizes time-range queries; typical for streaming data |
| **Iceberg format v2** | Enables row-level deletes and updates for GDPR compliance |

---

## Validation Rules

For data quality checks in the silver layer:

1. `sensor_id` > 0
2. `temperature` between -50.0 and 100.0 (physical limits)
3. `humidity` between 0.0 and 100.0
4. `latitude` between -90.0 and 90.0
5. `longitude` between -180.0 and 180.0
6. `status` in ('active', 'faulty', 'inactive')
7. `timestamp` > 0 and < current_timestamp
