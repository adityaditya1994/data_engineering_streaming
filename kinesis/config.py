# AWS Configuration for Kinesis and Firehose
import os

CONFIG = {
    'region': os.environ.get('AWS_REGION', 'eu-north-1'),
    'stream_name': os.environ.get('KINESIS_STREAM', 'clickstream-input-stream'),
    'delivery_stream_name': os.environ.get('FIREHOSE_STREAM', 'clickstream-firehose-delivery'),
    # S3 Bucket for Bronze Layer (Raw Data) - from environment or CloudFormation
    'bucket_name': os.environ.get('BRONZE_BUCKET', 'your-bronze-bucket-name'),
    's3_prefix': 'clickstream/raw/',
    # IAM Role for Firehose to access S3 and Kinesis
    'firehose_role_name': os.environ.get('FIREHOSE_ROLE', 'FirehoseDeliveryRole'),
    # Glue Catalog for Parquet schema (used by Firehose)
    'glue_database': os.environ.get('GLUE_DATABASE', 'analytics'),
    'glue_table': 'clickstream_raw'
}
