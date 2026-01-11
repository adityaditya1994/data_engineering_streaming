import boto3
import json
import time
from botocore.exceptions import ClientError
from config import CONFIG

def create_kinesis_stream(kinesis, stream_name):
    try:
        kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        print(f"Creating stream {stream_name}...")
        waiter = kinesis.get_waiter('stream_exists')
        waiter.wait(StreamName=stream_name)
        print(f"Stream {stream_name} created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Stream {stream_name} already exists.")
        else:
            raise

def create_iam_role(iam, role_name, bucket_name, stream_arn):
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "firehose.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        print(f"Role {role_name} created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"Role {role_name} already exists.")
        else:
            raise

    # Attach policies
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            },
            {
                "Sid": "KinesisAccess",
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards"
                ],
                "Resource": stream_arn
            }
        ]
    }

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName='FirehoseS3KinesisPolicy',
        PolicyDocument=json.dumps(policy_doc)
    )
    print("Policy attached to role.")
    
    # Get Role ARN
    role = iam.get_role(RoleName=role_name)
    return role['Role']['Arn']

def create_firehose(firehose, delivery_name, source_stream_arn, dest_bucket_arn, role_arn):
    try:
        firehose.create_delivery_stream(
            DeliveryStreamName=delivery_name,
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': source_stream_arn,
                'RoleARN': role_arn
            },
            ExtendedS3DestinationConfiguration={
                'RoleARN': role_arn,
                'BucketARN': dest_bucket_arn,
                'Prefix': CONFIG['s3_prefix'],
                'ErrorOutputPrefix': 'clickstream/errors/',
                'BufferingHints': {
                    'SizeInMBs': 64,
                    'IntervalInSeconds': 60
                },
                'CompressionFormat': 'UNCOMPRESSED',  # Parquet handles its own compression
                'DataFormatConversionConfiguration': {
                    'Enabled': True,
                    'SchemaConfiguration': {
                        'RoleARN': role_arn,
                        'DatabaseName': CONFIG.get('glue_database', 'analytics'),
                        'TableName': CONFIG.get('glue_table', 'clickstream_raw'),
                        'Region': CONFIG['region'],
                        'VersionId': 'LATEST'
                    },
                    'InputFormatConfiguration': {
                        'Deserializer': {
                            'OpenXJsonSerDe': {}
                        }
                    },
                    'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {
                                'Compression': 'SNAPPY'
                            }
                        }
                    }
                }
            }
        )
        print(f"Creating Firehose {delivery_name}...")
        
        # Wait for ACTIVE state
        while True:
            response = firehose.describe_delivery_stream(DeliveryStreamName=delivery_name)
            status = response['DeliveryStreamDescription']['DeliveryStreamStatus']
            if status == 'ACTIVE':
                print(f"Firehose {delivery_name} is active.")
                break
            elif status == 'CREATING':
                time.sleep(5)
            else:
                raise Exception(f"Firehose creation failed with status: {status}")
                
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Firehose {delivery_name} already exists.")
        else:
            raise

def main():
    region = CONFIG['region']
    kinesis = boto3.client('kinesis', region_name=region)
    iam = boto3.client('iam', region_name=region)
    firehose = boto3.client('firehose', region_name=region)

    print("--- Setting up Kinesis Data Stream ---")
    create_kinesis_stream(kinesis, CONFIG['stream_name'])
    
    # Get Stream ARN
    stream_desc = kinesis.describe_stream(StreamName=CONFIG['stream_name'])
    stream_arn = stream_desc['StreamDescription']['StreamARN']
    
    print("\n--- Setting up IAM Role ---")
    role_arn = create_iam_role(iam, CONFIG['firehose_role_name'], CONFIG['bucket_name'], stream_arn)
    
    # Wait for IAM propagation
    print("Waiting 10s for IAM propagation...")
    time.sleep(10)
    
    print("\n--- Setting up Firehose Delivery Stream ---")
    bucket_arn = f"arn:aws:s3:::{CONFIG['bucket_name']}"
    create_firehose(firehose, CONFIG['delivery_stream_name'], stream_arn, bucket_arn, role_arn)
    
    print("\n✅ Kinesis Infrastructure Setup Complete!")

if __name__ == "__main__":
    main()
