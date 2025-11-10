# Arquitetura MLOps Completa

## Visao Geral

Este template implementa uma esteira completa de MLOps integrada com AWS, seguindo as melhores praticas da industria para desenvolvimento, deploy e monitoramento de modelos de Machine Learning em producao.

## Stack Tecnologico

### Infraestrutura AWS
- **S3**: Armazenamento de dados (raw, processed, artifacts)
- **Athena**: Queries SQL no data lake
- **Iceberg**: Table format para analytics
- **EC2**: Instancias para treinamento e inferencia
- **ECR**: Registro de containers Docker
- **ECS**: Orquestracao de containers (opcional)

### MLOps Core
- **MLFlow**: Tracking de experimentos, model registry, model serving
- **Airflow**: Orquestracao de pipelines de dados e ML
- **FastAPI**: API REST para model serving
- **Docker**: Containerizacao de componentes

### Monitoramento
- **Evidently**: Deteccao de drift e quality monitoring
- **Prometheus**: Coleta de metricas
- **Grafana**: Visualizacao de metricas e dashboards

### Qualidade de Codigo
- **Ruff**: Linting e formatting
- **Mypy**: Type checking
- **Bandit**: Security scanning
- **Pytest**: Testing framework
- **Pre-commit**: Git hooks

## Arquitetura de Componentes

### 1. Data Layer

```
Raw Data (Bronze)
    |
    v
Data Processing (Silver)
    |
    v
Feature Engineering (Gold)
    |
    v
Feature Store (Iceberg/Athena)
```

**Componentes:**
- `src/data/aws_integration.py`: Integracao com S3, Athena, Iceberg
- `src/pipelines/DE/`: Pipelines de engenharia de dados
- `config/aws_config.yaml`: Configuracoes AWS

### 2. Training Layer

```
Feature Store
    |
    v
Training Pipeline
    |
    v
Model Evaluation
    |
    v
MLFlow Model Registry
    |
    v
Model Promotion (Staging/Production)
```

**Componentes:**
- `src/model/mlflow_manager.py`: Gerenciamento MLFlow
- `src/pipelines/DS/training_pipeline/`: Pipeline de treinamento
- `airflow/dags/ml_training_pipeline.py`: Orquestracao Airflow

**Fluxo de Treinamento:**
1. Extracao de features do Feature Store
2. Preprocessamento e split de dados
3. Treinamento com tracking MLFlow
4. Avaliacao de metricas
5. Registro no Model Registry
6. Promocao para Production (se aprovado)

### 3. Inference Layer

```
Production Data
    |
    v
Feature Engineering
    |
    v
Model Serving API (FastAPI)
    |
    v
Predictions
    |
    v
Monitoring & Logging
```

**Componentes:**
- `src/inference/api.py`: API REST FastAPI
- `src/inference/monitoring.py`: Monitoramento de predicoes
- `airflow/dags/ml_batch_inference_pipeline.py`: Inferencia em lote

**Modos de Inferencia:**
1. **Online (Real-time)**: API REST com FastAPI
2. **Batch**: Pipeline Airflow para predicoes em lote

### 4. Monitoring Layer

```
Predictions
    |
    v
Data Quality Checks
    |
    v
Drift Detection
    |
    v
Alerts & Notifications
```

**Metricas Monitoradas:**
- Performance do modelo (accuracy, F1, etc.)
- Latencia de inferencia
- Data drift
- Concept drift
- Volume de predicoes
- Erros e exceptions

## Pipelines MLOps

### Pipeline de Training

```yaml
Etapa 1: Data Extraction
  - Query features do Athena/Iceberg
  - Validacao de qualidade

Etapa 2: Feature Engineering
  - Transformacoes
  - Feature selection
  - Train/test split

Etapa 3: Model Training
  - Treinamento com MLFlow tracking
  - Hyperparameter tuning (Optuna)
  - Cross-validation

Etapa 4: Evaluation
  - Metricas de performance
  - Comparison com baseline
  - Analise de erros

Etapa 5: Registration
  - Upload para MLFlow Model Registry
  - Tagging e versionamento
  - Documentation

Etapa 6: Deployment
  - Promocao para Staging
  - Testes de integracao
  - Promocao para Production
```

### Pipeline de Inference

```yaml
Etapa 1: Data Loading
  - Carrega dados para inferencia
  - Validacao de schema

Etapa 2: Feature Engineering
  - Aplica mesmas transformacoes do training

Etapa 3: Model Loading
  - Carrega modelo Production do MLFlow

Etapa 4: Prediction
  - Gera predicoes
  - Logging de resultados

Etapa 5: Storage
  - Salva predicoes no S3
  - Atualiza tabelas Iceberg

Etapa 6: Monitoring
  - Detecta drift
  - Calcula metricas
  - Envia alertas
```

## CI/CD Pipeline

### Continuous Integration

```yaml
On Push/PR:
  1. Code Quality Checks
     - Ruff linting
     - Type checking (mypy)
     - Security scan (bandit)

  2. Testing
     - Unit tests
     - Integration tests
     - Coverage report

  3. Build
     - Docker images
     - Push to ECR
```

### Continuous Deployment

```yaml
On Main Branch:
  1. Model Training
     - Train on latest data
     - Validate performance

  2. Deploy to Staging
     - Update ECS service
     - Run smoke tests

  3. Deploy to Production
     - Promote model
     - Update ECS service
     - Monitor metrics
```

## Configuracao e Setup

### 1. Pre-requisitos

- Python 3.11+
- Docker e Docker Compose
- AWS CLI configurado
- Credenciais AWS com permissoes adequadas

### 2. Instalacao

```bash
# Clone o repositorio
git clone <repo-url>
cd {{ cookiecutter.repo_name }}

# Configure variaveis de ambiente
cp .env.example .env
# Edite .env com suas configuracoes

# Execute setup completo
make setup

# Inicie servicos
make docker-up
```

### 3. Configuracao AWS

```bash
# Crie recursos AWS
make init-aws

# Configure Athena database
aws athena start-query-execution \
  --query-string "CREATE DATABASE ml_database" \
  --result-configuration OutputLocation=s3://your-bucket/
```

### 4. Inicialize MLFlow

```bash
# Inicie MLFlow server
make mlflow-up

# Crie experimento
make init-mlflow

# Acesse UI: http://localhost:5000
```

## Uso

### Treinamento de Modelo

```bash
# Local
make train-local

# Com Airflow
make airflow-up
# Acesse: http://localhost:8080
# Trigger DAG: ml_training_pipeline
```

### Serving de Modelo

```bash
# Desenvolvimento
make serve

# Producao
make serve-prod

# Teste API
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": {"feature1": 1.0, "feature2": 2.0}}'
```

### Monitoramento

```bash
# Inicie stack de monitoramento
make monitoring-up

# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090

# Execute verificacao de drift
make drift-check
```

## Boas Praticas

### 1. Versionamento

- **Codigo**: Git + semantic versioning
- **Dados**: DVC + S3
- **Modelos**: MLFlow Model Registry
- **Features**: Feature Store com timestamps

### 2. Reproducibilidade

- Pin de versoes de dependencias
- Seeds aleatorios fixos
- Logging completo de experimentos
- Containerizacao

### 3. Testes

- Unit tests para funcoes criticas
- Integration tests para pipelines
- Model tests (performance, fairness)
- API tests (load, stress)

### 4. Seguranca

- Credentials via environment variables
- IAM roles para AWS
- Security scanning automatizado
- Audit logs

### 5. Monitoramento

- Metricas de negocio e tecnicas
- Alertas automaticos
- Dashboards em tempo real
- Incident response procedures

## Troubleshooting

### Problemas Comuns

1. **MLFlow nao conecta**
   - Verifique se o container esta rodando: `docker ps`
   - Verifique MLFLOW_TRACKING_URI no .env
   - Logs: `docker-compose logs mlflow`

2. **Erro de permissao AWS**
   - Verifique credenciais: `aws sts get-caller-identity`
   - Verifique IAM policies
   - Configure AWS CLI: `aws configure`

3. **Drift detectado**
   - Revise feature engineering
   - Considere retreinamento
   - Analise mudancas nos dados

4. **Performance degradada**
   - Monitore metricas de modelo
   - Verifique latencia de API
   - Analise logs de erro

## Referencias

- [MLOps Principles](https://ml-ops.org/)
- [CD4ML](https://martinfowler.com/articles/cd4ml.html)
- [MLFlow Documentation](https://mlflow.org/docs/latest/index.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [AWS Well-Architected ML](https://aws.amazon.com/architecture/well-architected/)

## Contato

Desenvolvido por {{ cookiecutter.author_name }}

Para duvidas ou sugestoes, abra uma issue no repositorio.
