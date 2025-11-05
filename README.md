# Cookiecutter - Template de Projeto de Data Science Rentcars

Este repositório contém um template completo e configurável para projetos de Data Science com boas práticas de organização, versionamento, estrutura de dados e pipelines.


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

## Autenticação com Flow Agents

Após a geração do projeto, siga os passos abaixo para configurar o token de acesso:

### 1. Instale as dependências

```bash
pip install -r requirements.txt
```

### 2. Execute o script de autenticação

```bash
python ./flow/flow_token.py
```

> Apenas aguarde a confirmação da autenticação — as credenciais serão salvas automaticamente.

### 3. Confirme a criação do `.env`

Após a execução correta, um arquivo `.env` será criado com as variáveis:

* `FLOW_TOKEN`
* `FLOW_TENANT`

---

## Pronto para desenvolver

A partir daqui, você pode:

* Criar notebooks em `notebooks/`
* Desenvolver pipelines modulares em `src/pipelines/`
* Armazenar dados em camadas no `data/`
* Usar o `flow/review_agent.py` ou `flow/refactor_agent.py` com suas credenciais

---

## Requisitos

* Python 3.10, 3.11 ou 3.12
* Git + cookiecutter
* Conta CI\&T com acesso ao Flow

---

## Autor

Template mantido por **[@RaphaelNorris](https://github.com/RaphaelNorris)**
