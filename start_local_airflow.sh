#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Starting Local Airflow Setup ===${NC}"

# Check for AWS CLI
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install it first."
    exit 1
fi

echo -e "${GREEN}Verifying AWS Stacks...${NC}"

# Get Bucket Names from CloudFormation
STACK_NAME="ahs-s3-stack-v1"
if ! aws cloudformation describe-stacks --stack-name $STACK_NAME --region eu-north-1 &> /dev/null; then
    echo "Error: Stack $STACK_NAME not found. Please deploy s3_buckets.yaml first."
    exit 1
fi

echo "Fetching S3 Bucket names..."
BRONZE_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region eu-north-1 --query 'Stacks[0].Outputs[?OutputKey==`BronzeBucketName`].OutputValue' --output text)
SCRIPTS_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region eu-north-1 --query 'Stacks[0].Outputs[?OutputKey==`ScriptsBucketName`].OutputValue' --output text)
LOGS_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region eu-north-1 --query 'Stacks[0].Outputs[?OutputKey==`LogsBucketName`].OutputValue' --output text)
ICEBERG_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region eu-north-1 --query 'Stacks[0].Outputs[?OutputKey==`IcebergBucketName`].OutputValue' --output text)

echo "Found Buckets:"
echo "  Bronze: $BRONZE_BUCKET"
echo "  Scripts: $SCRIPTS_BUCKET"
echo "  Logs: $LOGS_BUCKET"

# Generate .env files for both environments
echo -e "${GREEN}Generating .env files...${NC}"

generate_env() {
    cat > $1/.env <<EOF
AIRFLOW_UID=$(id -u)
AWS_DEFAULT_REGION=eu-north-1

# EMR Configuration
EMR_CLUSTER_ID=j-PLACEHOLDER
SCRIPT_BUCKET=$SCRIPTS_BUCKET
BRONZE_PATH=s3://$BRONZE_BUCKET/clickstream/
ICEBERG_DB=analytics
EMR_LOG_URI=s3://$LOGS_BUCKET/emr-logs/
EMR_EC2_ROLE=EMR_EC2_DefaultRole
EMR_ROLE=EMR_DefaultRole

# AWS Credentials
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
EOF
}

mkdir -p docker_airflow
mkdir -p docker_notebook

generate_env "docker_airflow"
generate_env "docker_notebook"

echo -e "${GREEN}Uploading Scripts to S3...${NC}"
aws s3 cp EMR/Pyspark/ s3://$SCRIPTS_BUCKET/ --recursive --exclude "*" --include "*.py"

echo -e "${GREEN}Generating Sample Data...${NC}"
aws s3 cp EMR/Pyspark/sample_clickstream.json s3://$BRONZE_BUCKET/clickstream/sample_clickstream.json

echo -e "${BLUE}=== Setup Complete ===${NC}"
echo "To start PROD Airflow:"
echo "  cd docker_airflow"
echo "  docker-compose up -d"
echo "  Access: http://localhost:8080 (admin/admin)"
echo ""
echo "To start DEV Notebook:"
echo "  cd docker_notebook"
echo "  docker-compose up -d"
echo "  Access: http://localhost:8888 (no token)"
