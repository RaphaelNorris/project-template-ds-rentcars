# Template MLOps Completo para Projetos de Machine Learning

Este repositorio contem um template completo de MLOps para projetos de Machine Learning em producao, com integracao AWS, MLFlow, Airflow, e monitoramento continuo.

## Principais Caracteristicas

- Stack AWS completo (S3, Athena, Iceberg, EC2, ECR)
- MLFlow para tracking de experimentos e model registry
- Airflow para orquestracao de pipelines
- FastAPI para model serving
- Monitoramento com Evidently, Prometheus e Grafana
- CI/CD completo com GitHub Actions
- Containerizacao com Docker
- Qualidade de codigo automatizada (Ruff, Mypy, Bandit)
- Feature Store com Iceberg
- Deteccao de drift automatica


---

## Inicio Rapido

```bash
# 1. Gere o projeto usando cookiecutter
cookiecutter https://github.com/RaphaelNorris/project-template-ds-rentcars.git

# 2. Entre no diretorio do projeto
cd seu-projeto

# 3. Configure variaveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais AWS

# 4. Execute setup completo
make setup

# 5. Inicie servicos
make docker-up

# 6. Acesse MLFlow
# http://localhost:5000

# 7. Acesse Grafana
# http://localhost:3000 (admin/admin)
```

---

## Como usar este template

### 1. Crie e ative um ambiente virtual

```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
# ou source venv/bin/activate (Linux/macOS)
````

### 2. Instale o `cookiecutter`

```bash
pip install cookiecutter
```

### 3. Gere seu projeto

```bash
cookiecutter https://github.com/RaphaelNorris/project-template-ds-rentcars.git
```

Você será guiado com prompts no terminal.

```
project_name: "Nome-do-Projeto"
repo_name: "Nome-Repos"
author_name: "Seu-Nome"
...
```

---

## Estrutura gerada

Exemplo do que será criado:

```
brad-seguros/
├── data/
│   ├── 01 - bronze/
│   ├── 02 - silver/
│   ├── 03 - ml/
│   └── 04 - gold/
├── notebooks/
├── src/
├── tests/
├── .gitignore
├── requirements.txt
└── README.md
```

---

Após a geração do projeto, siga os passos abaixo para configurar o token de acesso:

### 1. Instale as dependências

```bash
pip install -r requirements.txt
```

### 2. Execute o script de autenticação


> Apenas aguarde a confirmação da autenticação — as credenciais serão salvas automaticamente.

### 3. Confirme a criação do `.env`


---

## Pronto para desenvolver

A partir daqui, você pode:

* Criar notebooks em `notebooks/`
* Desenvolver pipelines modulares em `src/pipelines/`
* Armazenar dados em camadas no `data/`

---

## Requisitos

* Python 3.10, 3.11 ou 3.12
* Git + cookiecutter
* Docker e Docker Compose
* AWS CLI configurado
* Credenciais AWS com permissoes adequadas

---

## Arquitetura MLOps

Este template implementa uma arquitetura completa de MLOps com os seguintes componentes:

### Camadas da Arquitetura

1. **Data Layer**
   - Ingestao de dados (Bronze)
   - Processamento (Silver)
   - Features (Gold)
   - Feature Store (Iceberg)

2. **Training Layer**
   - Pipeline de treinamento automatizado
   - Tracking com MLFlow
   - Hyperparameter tuning com Optuna
   - Model Registry

3. **Inference Layer**
   - API REST com FastAPI
   - Batch inference com Airflow
   - Model serving otimizado

4. **Monitoring Layer**
   - Data drift detection
   - Model performance monitoring
   - Prometheus + Grafana
   - Alerting automatico

### Pipelines Disponiveis

- `ml_training_pipeline`: Pipeline completo de treinamento
- `ml_batch_inference_pipeline`: Inferencia em lote com monitoramento
- `feature_pipeline`: Engenharia de features
- `monitoring_pipeline`: Monitoramento e drift detection

### Comandos Uteis

```bash
# Treinamento
make train              # Treinar modelo
make tune               # Hyperparameter tuning
make evaluate           # Avaliar modelos

# Serving
make serve              # Iniciar API de inferencia
make predict            # Batch predictions

# Monitoramento
make monitor            # Monitorar modelos
make drift-check        # Verificar drift

# Infraestrutura
make docker-up          # Iniciar todos servicos
make mlflow-up          # Apenas MLFlow
make monitoring-up      # Prometheus + Grafana

# Deployment
make deploy-staging     # Deploy staging
make deploy-prod        # Deploy production
make promote-model      # Promover modelo
```

Para documentacao completa, veja [docs/mlops_architecture.md]({{ cookiecutter.repo_name }}/docs/mlops_architecture.md)

---

## Integracao com Stack Existente

Este template foi projetado para integrar com sua esteira DataOps existente:

- **Airflow**: Os DAGs de ML podem ser integrados com seus DAGs de DBT/DataMarts
- **S3**: Usa mesma estrutura de buckets e paths
- **Athena/Iceberg**: Consulta dados processados pela engenharia de dados
- **Monitoramento**: Dashboards unificados no Grafana

---

## Autor

Template mantido por **[@RaphaelNorris](https://github.com/RaphaelNorris)**
