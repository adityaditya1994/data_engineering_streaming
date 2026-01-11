"""
Centralized Configuration for Clickstream ETL Pipeline

This file contains all environment-specific configuration values.
Team members should set these via environment variables or a .env file.

Usage:
    from config.project_config import Config
    print(Config.BRONZE_BUCKET)
"""

import os


class Config:
    """Project-wide configuration from environment variables."""
    
    # AWS Settings
    AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '')
    
    # S3 Buckets (typically from CloudFormation stack outputs)
    BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET', '')
    ICEBERG_BUCKET = os.environ.get('ICEBERG_BUCKET', '')
    SCRIPTS_BUCKET = os.environ.get('SCRIPTS_BUCKET', '')
    LOGS_BUCKET = os.environ.get('LOGS_BUCKET', '')
    
    # Derived S3 Paths
    @classmethod
    def get_bronze_path(cls):
        return f"s3://{cls.BRONZE_BUCKET}/clickstream/"
    
    @classmethod
    def get_iceberg_warehouse(cls):
        return f"s3://{cls.ICEBERG_BUCKET}/"
    
    # Glue/Iceberg Settings
    GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'analytics')
    
    # Kinesis Settings
    KINESIS_STREAM = os.environ.get('KINESIS_STREAM', 'clickstream-input-stream')
    FIREHOSE_STREAM = os.environ.get('FIREHOSE_STREAM', 'clickstream-firehose-delivery')
    FIREHOSE_ROLE = os.environ.get('FIREHOSE_ROLE', 'FirehoseDeliveryRole')
    
    # EMR Settings
    EMR_LOG_URI = os.environ.get('EMR_LOG_URI', '')
    EMR_EC2_ROLE = os.environ.get('EMR_EC2_ROLE', 'EMR_EC2_DefaultRole')
    EMR_SERVICE_ROLE = os.environ.get('EMR_ROLE', 'EMR_DefaultRole')
    
    @classmethod
    def validate(cls):
        """Validate required configuration is set."""
        required = ['BRONZE_BUCKET', 'ICEBERG_BUCKET', 'SCRIPTS_BUCKET']
        missing = [k for k in required if not getattr(cls, k)]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}. "
                           f"Set these environment variables or create a .env file.")
        return True
