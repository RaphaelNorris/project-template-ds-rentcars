# Guia de Setup MLOps

Este guia detalha como configurar e iniciar a esteira MLOps completa.

## Pre-requisitos

### Ferramentas Necessarias

```bash
# Verifique as versoes instaladas
python --version    # 3.11+
docker --version    # 20.10+
docker-compose --version  # 2.0+
aws --version       # 2.0+
git --version       # 2.0+
```

### Permissoes AWS

Sua conta AWS precisa das seguintes permissoes:

- S3: Criar buckets, ler/escrever objetos
- Athena: Criar databases, executar queries
- Glue: Criar/gerenciar catalogs (para Iceberg)
- ECR: Criar repositories, push/pull images
- EC2: Lancar instancias (se usar para treinamento)
- ECS: Gerenciar services e tasks (se usar para deploy)
- IAM: Criar roles e policies

## Passo a Passo

### 1. Configuracao Inicial

```bash
# Clone ou gere o projeto
cookiecutter https://github.com/RaphaelNorris/project-template-ds-rentcars.git

# Entre no diretorio
cd seu-projeto

# Crie ambiente virtual
python -m venv .venv
source .venv/bin/activate  # ou .venv\Scripts\activate no Windows

# Instale dependencias
pip install -r requirements.txt
```

### 2. Configuracao AWS

```bash
# Configure AWS CLI
aws configure
# AWS Access Key ID: <sua-key>
# AWS Secret Access Key: <seu-secret>
# Default region name: us-east-1
# Default output format: json

# Teste conexao
aws sts get-caller-identity
```

### 3. Variaveis de Ambiente

```bash
# Copie template
cp .env.example .env

# Edite .env com suas configuracoes
nano .env  # ou vim, code, etc.
```

Configuracoes minimas necessarias:

```env
# AWS
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<sua-key>
AWS_SECRET_ACCESS_KEY=<seu-secret>

# S3 Buckets
S3_RAW_BUCKET=seu-bucket-raw
S3_PROCESSED_BUCKET=seu-bucket-processed
S3_ML_ARTIFACTS_BUCKET=seu-bucket-ml-artifacts

# Athena
ATHENA_DATABASE=ml_database
ATHENA_OUTPUT_LOCATION=s3://seu-bucket-athena-results/

# MLFlow
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=production_experiment
```

### 4. Setup Automatico

```bash
# Execute script de setup
chmod +x scripts/setup_mlops.sh
./scripts/setup_mlops.sh
```

O script automaticamente:
- Instala dependencias
- Cria buckets S3
- Cria database Athena
- Cria repositories ECR
- Inicia MLFlow server
- Configura pre-commit hooks

### 5. Verificacao

```bash
# Verifique se servicos estao rodando
docker-compose ps

# Deve mostrar:
# - mlops-postgres (healthy)
# - mlops-mlflow (healthy)

# Acesse MLFlow UI
open http://localhost:5000

# Verifique logs
docker-compose logs mlflow
```

## Configuracao Avancada

### MLFlow com Backend PostgreSQL

Para producao, use PostgreSQL em vez de SQLite:

```yaml
# docker-compose.yaml ja esta configurado
# Variaveis de ambiente:
MLFLOW_BACKEND_STORE_URI=postgresql://mlflow:mlflow@postgres:5432/mlflow
```

### Feature Store com Feast

```bash
# Instale Feast
pip install feast

# Inicialize feature store
feast init feature_store

# Configure feature_store/feature_store.yaml
# Veja: config/aws_config.yaml para configuracoes
```

### Airflow Setup

```bash
# Inicialize Airflow
make airflow-init

# Inicie servicos
make airflow-up

# Acesse UI
open http://localhost:8080
# Login: admin / admin
```

### Monitoramento

```bash
# Inicie stack de monitoramento
make monitoring-up

# Acesse Grafana
open http://localhost:3000
# Login: admin / admin

# Acesse Prometheus
open http://localhost:9090
```

## Troubleshooting

### Problema: Docker nao inicia

**Sintoma**: `docker-compose up` falha

**Solucao**:
```bash
# Verifique se Docker esta rodando
docker ps

# Limpe recursos antigos
docker system prune -f

# Reconstrua imagens
docker-compose build --no-cache
```

### Problema: MLFlow nao conecta

**Sintoma**: Erro "Connection refused" ao usar MLFlow

**Solucao**:
```bash
# Verifique se MLFlow esta rodando
docker-compose ps mlflow

# Verifique logs
docker-compose logs mlflow

# Reinicie servico
docker-compose restart mlflow

# Aguarde servico ficar healthy
docker-compose ps mlflow
```

### Problema: Erro de permissao AWS

**Sintoma**: "Access Denied" ao acessar S3/Athena

**Solucao**:
```bash
# Verifique credenciais
aws sts get-caller-identity

# Teste acesso a S3
aws s3 ls

# Verifique IAM policies
aws iam get-user-policy --user-name seu-usuario --policy-name sua-policy
```

### Problema: Athena query falha

**Sintoma**: Erro ao executar query no Athena

**Solucao**:
```bash
# Verifique se database existe
aws athena list-databases --catalog-name AwsDataCatalog

# Verifique output location
aws athena get-work-group --work-group-name primary

# Teste query simples
aws athena start-query-execution \
  --query-string "SELECT 1" \
  --result-configuration OutputLocation=s3://seu-bucket/
```

### Problema: Import error em Python

**Sintoma**: ModuleNotFoundError

**Solucao**:
```bash
# Verifique PYTHONPATH
echo $PYTHONPATH

# Configure PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Ou adicione ao .env
echo "PYTHONPATH=$(pwd)" >> .env
```

### Problema: Drift detectado constantemente

**Sintoma**: Alertas frequentes de drift

**Solucao**:
1. Revise threshold de drift em `config/aws_config.yaml`
2. Verifique qualidade dos dados de referencia
3. Considere retreinamento se drift eh legitimo
4. Atualize dados de referencia periodicamente

## Validacao Final

Execute checklist completo:

```bash
# 1. Servicos Docker
docker-compose ps

# 2. Conexao AWS
aws s3 ls

# 3. MLFlow
curl http://localhost:5000/health

# 4. API de inferencia (se rodando)
curl http://localhost:8000/health

# 5. Prometheus
curl http://localhost:9090/-/healthy

# 6. Grafana
curl http://localhost:3000/api/health

# 7. Testes
make test

# 8. Qualidade de codigo
make quality
```

Se todos passarem, seu ambiente esta pronto!

## Proximos Passos

1. Explore notebooks em `notebooks/`
2. Configure seus pipelines em `src/pipelines/`
3. Ajuste configuracoes em `config/`
4. Execute primeiro treinamento: `make train-local`
5. Suba API de inferencia: `make serve`
6. Configure dashboards no Grafana
7. Integre com sua esteira DataOps existente

## Recursos Adicionais

- [Documentacao MLFlow](https://mlflow.org/docs/latest/index.html)
- [Documentacao Airflow](https://airflow.apache.org/docs/)
- [AWS MLOps Best Practices](https://aws.amazon.com/blogs/machine-learning/)
- [Evidently Documentation](https://docs.evidentlyai.com/)

## Suporte

Para problemas nao resolvidos:

1. Verifique logs: `docker-compose logs`
2. Consulte documentacao em `docs/`
3. Abra issue no repositorio
4. Entre em contato com o time
