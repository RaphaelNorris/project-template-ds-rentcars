# Documentacao do Template MLOps

Esta pasta contem toda a documentacao tecnica do template MLOps.

## Documentos Disponiveis

### 1. [Arquitetura MLOps](mlops_architecture.md)
Documentacao completa da arquitetura MLOps, incluindo:
- Visao geral do stack tecnologico
- Camadas da arquitetura (Data, Training, Inference, Monitoring)
- Descricao detalhada dos pipelines
- Fluxo de CI/CD
- Boas praticas e referencias

### 2. [Guia de Setup](setup_guide.md)
Guia passo a passo para configurar o ambiente:
- Pre-requisitos
- Configuracao AWS
- Setup de servicos (MLFlow, Airflow, Prometheus, Grafana)
- Troubleshooting
- Validacao do ambiente

### 3. [Estrutura de Dados](data_structure.md)
Documentacao da estrutura de dados em camadas:
- Bronze (raw)
- Silver (processed)
- Gold (analytics)
- ML (features, models, predictions)

### 4. [Fontes de Dados](data_source.md)
Informacoes sobre as fontes de dados do projeto

### 5. [Pipelines](pipelines.md)
Documentacao dos pipelines de dados e ML

### 6. [Resultados](results.md)
Documentacao de resultados e metricas dos modelos

## Estrutura de Pastas

```
docs/
├── README.md                    # Este arquivo
├── mlops_architecture.md        # Arquitetura completa
├── setup_guide.md              # Guia de setup
├── data_structure.md           # Estrutura de dados
├── data_source.md              # Fontes de dados
├── pipelines.md                # Documentacao de pipelines
├── results.md                  # Resultados e metricas
└── index.md                    # Indice principal
```

## Como Usar

### Para Desenvolvedores

1. Comece com [setup_guide.md](setup_guide.md) para configurar seu ambiente
2. Leia [mlops_architecture.md](mlops_architecture.md) para entender a arquitetura
3. Consulte documentos especificos conforme necessario

### Para Cientistas de Dados

1. Revise [data_structure.md](data_structure.md) para entender a organizacao dos dados
2. Consulte [pipelines.md](pipelines.md) para entender os pipelines disponiveis
3. Use [results.md](results.md) como template para documentar seus experimentos

### Para Engenheiros de ML

1. Estude [mlops_architecture.md](mlops_architecture.md) em detalhes
2. Configure ambiente seguindo [setup_guide.md](setup_guide.md)
3. Implemente pipelines customizados baseados nos exemplos

## Geracao de Documentacao Web

Este projeto suporta geracao de documentacao com MkDocs:

```bash
# Instalar MkDocs
pip install mkdocs mkdocs-material

# Servir localmente
mkdocs serve

# Acessar em http://localhost:8000

# Build para producao
mkdocs build

# Deploy para GitHub Pages
mkdocs gh-deploy
```

## Contribuindo com Documentacao

Ao adicionar novos componentes ou funcionalidades:

1. Atualize a documentacao relevante
2. Adicione exemplos de uso
3. Inclua diagramas quando apropriado
4. Mantenha o formato markdown consistente
5. Atualize este README se adicionar novos documentos

## Convencoes

- Use markdown (.md) para todos os documentos
- Mantenha linhas com maximo 100 caracteres
- Use blocos de codigo com syntax highlighting
- Inclua links relativos entre documentos
- Adicione indices no inicio de documentos longos

## Recursos Externos

### MLOps
- [MLOps Principles](https://ml-ops.org/)
- [Google MLOps](https://cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning)
- [AWS MLOps](https://aws.amazon.com/sagemaker/mlops/)

### Ferramentas
- [MLFlow Docs](https://mlflow.org/docs/latest/index.html)
- [Airflow Docs](https://airflow.apache.org/docs/)
- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [Evidently Docs](https://docs.evidentlyai.com/)

### AWS
- [S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html)
- [Athena Guide](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)
- [Iceberg on AWS](https://iceberg.apache.org/)

## Contato

Para duvidas sobre a documentacao:
- Abra uma issue no repositorio
- Entre em contato com {{ cookiecutter.author_name }}
