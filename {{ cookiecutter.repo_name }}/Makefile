.PHONY: help install install-dev setup test lint format type-check security clean
.PHONY: docker-build docker-up docker-down mlflow-up serve train predict monitor
.PHONY: deploy-staging deploy-prod init-aws init-mlflow

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
PROJECT_NAME := {{ cookiecutter.project_name }}
AWS_REGION := us-east-1

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ========================================
# Environment Setup
# ========================================

install: ## Install production dependencies
	$(PIP) install -r requirements.txt

install-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	pre-commit install

setup: ## Setup complete MLOps environment
	@echo "Setting up MLOps environment..."
	chmod +x scripts/setup_mlops.sh
	./scripts/setup_mlops.sh

init-aws: ## Initialize AWS resources (S3, Athena, ECR)
	@echo "Initializing AWS resources..."
	aws s3 mb s3://$(S3_RAW_BUCKET) --region $(AWS_REGION) || true
	aws s3 mb s3://$(S3_PROCESSED_BUCKET) --region $(AWS_REGION) || true
	aws s3 mb s3://$(S3_ML_ARTIFACTS_BUCKET) --region $(AWS_REGION) || true
	aws ecr create-repository --repository-name ml-training --region $(AWS_REGION) || true
	aws ecr create-repository --repository-name ml-inference --region $(AWS_REGION) || true

init-mlflow: ## Initialize MLFlow experiment
	$(PYTHON) -c "import mlflow; import os; mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI')); mlflow.create_experiment(os.getenv('MLFLOW_EXPERIMENT_NAME'))"

# ========================================
# Code Quality
# ========================================

lint: ## Run code linting with Ruff
	ruff check src/ tests/ --fix

format: ## Format code with Ruff
	ruff format src/ tests/

type-check: ## Run type checking with mypy
	mypy src/ --config-file .code_quality/mypy.ini

security: ## Run security checks with bandit
	bandit -r src/ -c .code_quality/bandit.yaml

quality: lint format type-check security ## Run all code quality checks

# ========================================
# Testing
# ========================================

test: ## Run tests with pytest
	pytest tests/ -v

test-cov: ## Run tests with coverage
	pytest tests/ --cov=src --cov-report=html --cov-report=term

test-integration: ## Run integration tests
	pytest tests/integration/ -v

# ========================================
# Docker
# ========================================

docker-build: ## Build Docker images
	docker-compose build

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all services
	docker-compose down

docker-logs: ## Show Docker logs
	docker-compose logs -f

mlflow-up: ## Start MLFlow server
	docker-compose up -d mlflow postgres

# ========================================
# ML Pipeline Commands
# ========================================

train: ## Run training pipeline
	$(PYTHON) -m src.pipelines.DS.training_pipeline.train

train-local: ## Run training pipeline locally
	MLFLOW_TRACKING_URI=http://localhost:5000 $(PYTHON) -m src.pipelines.DS.training_pipeline.train

tune: ## Run hyperparameter tuning
	$(PYTHON) scripts/tune_hyperparameters.py

evaluate: ## Evaluate trained models
	$(PYTHON) scripts/evaluate_models.py

# ========================================
# Model Serving
# ========================================

serve: ## Start model serving API
	uvicorn src.inference.api:app --host 0.0.0.0 --port 8000 --reload

serve-prod: ## Start model serving API in production mode
	uvicorn src.inference.api:app --host 0.0.0.0 --port 8000 --workers 4

predict: ## Make batch predictions
	$(PYTHON) scripts/batch_predict.py

# ========================================
# Monitoring
# ========================================

monitor: ## Run monitoring pipeline
	$(PYTHON) scripts/monitor_model.py

drift-check: ## Check for data drift
	$(PYTHON) scripts/check_drift.py

prometheus-up: ## Start Prometheus
	docker-compose up -d prometheus

grafana-up: ## Start Grafana
	docker-compose up -d grafana

monitoring-up: prometheus-up grafana-up ## Start all monitoring services

# ========================================
# Airflow
# ========================================

airflow-init: ## Initialize Airflow database
	docker-compose up -d postgres
	sleep 5
	docker-compose run --rm airflow-webserver airflow db init

airflow-up: ## Start Airflow services
	docker-compose up -d airflow-webserver airflow-scheduler

airflow-down: ## Stop Airflow services
	docker-compose stop airflow-webserver airflow-scheduler

# ========================================
# Deployment
# ========================================

deploy-staging: ## Deploy to staging environment
	@echo "Deploying to staging..."
	aws ecs update-service --cluster ml-staging-cluster --service ml-inference-service --force-new-deployment --region $(AWS_REGION)

deploy-prod: ## Deploy to production environment
	@echo "Deploying to production..."
	aws ecs update-service --cluster ml-production-cluster --service ml-inference-service --force-new-deployment --region $(AWS_REGION)

promote-model: ## Promote model to production
	$(PYTHON) scripts/promote_model.py --stage Production

# ========================================
# Data Management
# ========================================

sync-data: ## Sync data from S3
	aws s3 sync s3://$(S3_PROCESSED_BUCKET)/features/ data/ml/features/

upload-data: ## Upload data to S3
	aws s3 sync data/ml/features/ s3://$(S3_PROCESSED_BUCKET)/features/

create-feature-store: ## Create feature store tables
	$(PYTHON) scripts/create_feature_store.py

# ========================================
# Documentation
# ========================================

docs-serve: ## Serve documentation locally
	mkdocs serve

docs-build: ## Build documentation
	mkdocs build

docs-deploy: ## Deploy documentation to GitHub Pages
	mkdocs gh-deploy

# ========================================
# Cleanup
# ========================================

clean: ## Clean temporary files and caches
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/

clean-docker: ## Clean Docker resources
	docker-compose down -v
	docker system prune -f

clean-all: clean clean-docker ## Clean everything

# ========================================
# Development
# ========================================

notebook: ## Start Jupyter notebook server
	jupyter notebook notebooks/

lab: ## Start Jupyter lab
	jupyter lab

shell: ## Start Python shell with project context
	$(PYTHON) -i -c "from src.data.aws_integration import *; from src.model.mlflow_manager import *"

# ========================================
# CI/CD
# ========================================

ci-test: quality test ## Run CI tests

ci-build: docker-build ## Build for CI

ci-deploy: ## Deploy via CI
	@echo "Deploying via CI/CD pipeline..."
