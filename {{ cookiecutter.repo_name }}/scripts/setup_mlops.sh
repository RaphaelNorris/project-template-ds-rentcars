#!/bin/bash

# MLOps Environment Setup Script
# This script sets up the complete MLOps infrastructure

set -e

echo "=========================================="
echo "MLOps Environment Setup"
echo "=========================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "Please configure .env file with your AWS credentials and settings"
    exit 1
fi

# Load environment variables
source .env

echo "1. Installing Python dependencies..."
pip install -r requirements.txt

echo "2. Setting up AWS configuration..."
aws configure set region $AWS_REGION
aws configure set output json

echo "3. Creating S3 buckets if they don't exist..."
aws s3 mb s3://$S3_RAW_BUCKET --region $AWS_REGION 2>/dev/null || echo "Bucket $S3_RAW_BUCKET already exists"
aws s3 mb s3://$S3_PROCESSED_BUCKET --region $AWS_REGION 2>/dev/null || echo "Bucket $S3_PROCESSED_BUCKET already exists"
aws s3 mb s3://$S3_ML_ARTIFACTS_BUCKET --region $AWS_REGION 2>/dev/null || echo "Bucket $S3_ML_ARTIFACTS_BUCKET already exists"

echo "4. Creating Athena database..."
aws athena start-query-execution \
    --query-string "CREATE DATABASE IF NOT EXISTS $ATHENA_DATABASE" \
    --result-configuration "OutputLocation=$ATHENA_OUTPUT_LOCATION" \
    --region $AWS_REGION || echo "Database already exists"

echo "5. Creating ECR repositories..."
aws ecr create-repository --repository-name ml-training --region $AWS_REGION 2>/dev/null || echo "Repository ml-training already exists"
aws ecr create-repository --repository-name ml-inference --region $AWS_REGION 2>/dev/null || echo "Repository ml-inference already exists"
aws ecr create-repository --repository-name ml-feature-pipeline --region $AWS_REGION 2>/dev/null || echo "Repository ml-feature-pipeline already exists"

echo "6. Starting MLFlow server with Docker Compose..."
docker-compose up -d mlflow postgres

echo "7. Waiting for MLFlow server to be ready..."
sleep 10

echo "8. Creating MLFlow experiment..."
python -c "
import mlflow
import os

mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI'))
try:
    mlflow.create_experiment(
        name=os.getenv('MLFLOW_EXPERIMENT_NAME'),
        artifact_location=f\"s3://{os.getenv('S3_ML_ARTIFACTS_BUCKET')}/mlflow/\"
    )
    print(f\"Created experiment: {os.getenv('MLFLOW_EXPERIMENT_NAME')}\")
except:
    print(f\"Experiment {os.getenv('MLFLOW_EXPERIMENT_NAME')} already exists\")
"

echo "9. Setting up pre-commit hooks..."
pre-commit install

echo "=========================================="
echo "MLOps Environment Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Configure your .env file with correct values"
echo "2. Run 'docker-compose up -d' to start all services"
echo "3. Access MLFlow UI at http://localhost:5000"
echo "4. Access Grafana at http://localhost:3000 (admin/admin)"
echo "5. Start developing your ML pipelines!"
echo ""
echo "Useful commands:"
echo "  - make train: Run training pipeline"
echo "  - make serve: Start inference API"
echo "  - make test: Run tests"
echo "  - make lint: Run code quality checks"
