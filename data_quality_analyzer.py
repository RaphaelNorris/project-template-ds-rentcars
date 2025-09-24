import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# Carrega variáveis de ambiente a partir do arquivo .env
load_dotenv()

def get_connection_sqlserver():
    """
    Tenta estabelecer uma conexão com o SQL Server utilizando múltiplos drivers.
    Retorna a conexão e o método utilizado. Se nenhum método funcionar, retorna (None, None).
    """
    server = os.getenv('SQLSERVER_HOST')
    database = os.getenv('SQLSERVER_DATABASE')
    username = os.getenv('SQLSERVER_USER')
    password = os.getenv('SQLSERVER_PASSWORD')

    # Tenta conexão via pymssql (ideal para ambientes Linux)
    try:
        import pymssql
        print("Tentando pymssql...")
        conn = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            timeout=10
        )
        print("SUCESSO: Conectado via pymssql")
        return conn, 'pymssql'
    except Exception as e:
        print(f"pymssql falhou: {str(e)[:50]}")

    # Tenta conexão via pyodbc com diferentes drivers
    try:
        import pyodbc
        drivers = [
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 13 for SQL Server',
            'FreeTDS',
            'SQL Server'
        ]
        for driver in drivers:
            try:
                print(f"Tentando pyodbc com {driver}...")
                conn_string = (
                    f'DRIVER={{{driver}}};'
                    f'SERVER={server};'
                    f'DATABASE={database};'
                    f'UID={username};'
                    f'PWD={password};'
                    f'TrustServerCertificate=yes;'
                )
                conn = pyodbc.connect(conn_string, timeout=10)
                print(f"SUCESSO: Conectado via pyodbc + {driver}")
                return conn, 'pyodbc'
            except Exception:
                # Se a tentativa falhar, continua para o próximo driver
                continue
    except ImportError:
        print("pyodbc não instalado")

    # Nenhum método funcionou
    return None, None

def query_sqlserver_safe(query, params=None):
    """
    Executa uma consulta no SQL Server de forma segura, com suporte a parâmetros e
    tentativa de múltiplos métodos de conexão.

    Args:
        query (str): Query SQL a ser executada. Use '?' para parâmetros com pyodbc ou %s para pymssql.
        params (tuple, opcional): Parâmetros para a query. Default: None.

    Returns:
        pandas.DataFrame: DataFrame com os resultados da consulta ou vazio em caso de falha.
    """
    conn, _ = get_connection_sqlserver()
    if not conn:
        print("ERRO: Não foi possível conectar ao SQL Server")
        return pd.DataFrame()
    try:
        if params:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erro na query: {e}")
        conn.close()
        return pd.DataFrame()

def build_filtered_query(table_name, schema='dbo', filters=None, columns=None):
    """
    Constrói uma query SQL dinâmicamente, permitindo seleção de colunas e aplicação de filtros parametrizados.

    Args:
        table_name (str): Nome da tabela a ser consultada.
        schema (str): Schema da tabela. Default: 'dbo'.
        filters (dict, opcional): Dicionário com filtros no formato {coluna: {operator: operador, value: valor}}.
        columns (list, opcional): Lista de colunas a serem selecionadas. Default: None (todas as colunas).

    Returns:
        tuple: Uma tupla contendo a query montada e a tupla de parâmetros.
    """
    # Monta o SELECT
    select_clause = ', '.join(columns) if columns else '*'
    query = f"SELECT {select_clause} FROM {schema}.{table_name}"
    params = []
    # Aplica filtros se fornecidos
    if filters:
        where_conditions = []
        for col_name, filter_config in filters.items():
            operator = filter_config.get('operator', '=')
            value = filter_config['value']
            # Valida operadores seguros
            safe_operators = ['=', '!=', '>', '>=', '<', '<=', 'LIKE', 'IN', 'NOT IN']
            if operator.upper() not in safe_operators:
                raise ValueError(f"Operador não permitido: {operator}")
            if operator.upper() in ['IN', 'NOT IN']:
                # Para operadores IN e NOT IN, value deve ser uma lista
                if not isinstance(value, (list, tuple)):
                    value = [value]
                placeholders = ', '.join(['?' for _ in value])
                clause = f"{col_name} {operator} ({placeholders})"
                where_conditions.append(clause)
                params.extend(value)
            else:
                where_conditions.append(f"{col_name} {operator} ?")
                params.append(value)
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
    return query, tuple(params)

def generate_markdown_report(all_column_analysis, table_name, schema, filters, 
                           columns_to_exclude, exclusion_reasons, query, params):
    """
    Gera um relatório em Markdown descrevendo os resultados da análise de qualidade de dados.
    Inclui estilo CSS inline para tema escuro e destaca visivelmente as ações de manter ou excluir colunas.

    Args:
        all_column_analysis (list): Lista de dicionários com os dados de análise de cada coluna.
        table_name (str): Nome da tabela analisada.
        schema (str): Schema da tabela.
        filters (dict): Filtros aplicados para a extração de dados.
        columns_to_exclude (list): Lista de colunas marcadas para exclusão.
        exclusion_reasons (dict): Dicionário com os motivos para exclusão de cada coluna.
        query (str): Query SQL executada.
        params (tuple): Parâmetros utilizados na query.

    Returns:
        str: Conteúdo em Markdown formatado com estilo dark theme.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # CSS inline para tema escuro e formatação das tabelas
    markdown = """<style>
    body { background-color: #1e1e1e; color: #e5e5e5; font-family: Arial, sans-serif; }
    h1, h2, h3 { color: #ffcc00; }
    table { border-collapse: collapse; width: 100%; margin-top: 10px; }
    th, td { border: 1px solid #555; padding: 6px; text-align: center; }
    th { background-color: #333; color: #ffcc00; }
    td { color: #ddd; }
    .manter { color: #00cc66; font-weight: bold; }
    .excluir { color: #ff4444; font-weight: bold; }
    code { background-color: #2d2d2d; padding: 2px 4px; border-radius: 4px; }
    </style>\n\n"""
    # Informações gerais
    markdown += (f"# Relatório de Análise de Qualidade de Dados\n\n"
                 f"## Informações Gerais\n"
                 f"- **Tabela:** `{schema}.{table_name}`\n"
                 f"- **Data/Hora:** {timestamp}\n"
                 f"- **Total de Registros:** {len(all_column_analysis[0]['Dataframe']) if all_column_analysis else 'N/A'}\n"
                 f"- **Total de Colunas:** {len(all_column_analysis)}\n\n")
    # Filtros aplicados
    markdown += "---\n\n## Filtros Aplicados\n"
    if filters:
        for col, filter_config in filters.items():
            op = filter_config['operator']
            val = filter_config['value']
            markdown += f"- **{col}** {op} `{val}`\n"
    else:
        markdown += "- Nenhum filtro aplicado\n"
    # Query executada
    markdown += ("\n---\n\n## Query Executada\n"
                 "```sql\n"
                 f"{query}\n"
                 "```\n")
    if params:
        markdown += f"**Parâmetros:** {params}\n\n"
    # Resumo da análise
    total_cols = len(all_column_analysis)
    exclude_count = len(columns_to_exclude)
    keep_count = total_cols - exclude_count
    markdown += ("\n---\n\n## Resumo da Análise\n\n"
                 "| Métrica | Valor |\n"
                 "|---------|-------|\n"
                 f"| Total de Colunas | {total_cols} |\n"
                 f"| Colunas para Manter | {keep_count} |\n"
                 f"| Colunas para Excluir | {exclude_count} |\n"
                 f"| Percentual de Exclusão | {(exclude_count/total_cols)*100:.1f}% |\n\n")
    # Colunas sugeridas para exclusão
    if columns_to_exclude:
        markdown += ("---\n\n## Colunas Sugeridas para Exclusão\n\n"
                     "| # | Coluna | Motivos |\n"
                     "|---|--------|---------|\n")
        for i, col in enumerate(columns_to_exclude, 1):
            reasons_text = " | ".join(exclusion_reasons[col])
            markdown += f"| {i} | `{col}` | {reasons_text} |\n"
    else:
        markdown += ("---\n\n## Resultado da Análise\n\n"
                     "Nenhuma coluna precisa ser excluída.\n"
                     "Todas as colunas atendem aos critérios de qualidade definidos.\n")
    # Detalhamento de todas as colunas
    markdown += ("\n---\n\n## Análise Detalhada de Todas as Colunas\n\n"
                 "| Coluna | Ação | Nulos (%) | Valores Únicos | Variância (%) | Zeros (%) | Vazias (%) | Tipo | Motivos |\n"
                 "|--------|------|-----------|----------------|---------------|-----------|------------|------|---------|\n")
    for col_data in all_column_analysis:
        action_cell = ("<span class='excluir'>EXCLUIR</span>" if col_data['Acao'] == 'EXCLUIR'
                       else "<span class='manter'>MANTER</span>")
        markdown += (f"| `{col_data['Coluna']}` | {action_cell} | "
                     f"{col_data['Nulos_Percent']}% | "
                     f"{col_data['Valores_Unicos']} | "
                     f"{col_data['Variancia_Percent']}% | "
                     f"{col_data['Zeros_Percent']}% | "
                     f"{col_data['Vazias_Percent']}% | "
                     f"`{col_data['Tipo_Dados']}` | "
                     f"{col_data['Motivos']} |\n")
    # Critérios utilizados
    markdown += ("\n---\n\n## Critérios de Exclusão Utilizados\n\n"
                 "- Muitos Nulos: > 90% de valores nulos\n"
                 "- Valor Único: coluna possui apenas 1 valor único\n"
                 "- Muitos Zeros: > 80% de valores zero (para colunas numéricas)\n"
                 "- Strings Vazias: > 80% de strings vazias (para colunas de texto)\n\n"
                 "Colunas que não atendem a esses critérios são mantidas.\n")
    return markdown

def identify_columns_to_exclude(table_name, schema='dbo', filters=None, 
                               null_threshold=90, zero_threshold=80):
    """
    Realiza a análise de exclusão de colunas com base em critérios de qualidade.

    Args:
        table_name (str): Nome da tabela a ser analisada.
        schema (str): Schema da tabela. Default: 'dbo'.
        filters (dict, opcional): Filtros aplicados na consulta SQL. Default: None.
        null_threshold (int): Percentual mínimo de nulos para considerar a coluna candidata à exclusão. Default: 90.
        zero_threshold (int): Percentual mínimo de zeros/vazios para considerar a coluna candidata à exclusão. Default: 80.

    Returns:
        dict: Dicionário com informações sobre as colunas a manter e excluir, além do relatório gerado.
    """
    print("\nIDENTIFICANDO COLUNAS PARA EXCLUSÃO")
    print(f"Tabela: {schema}.{table_name}")
    # Exibe filtros
    if filters:
        print("Filtros aplicados:")
        for col, filter_config in filters.items():
            print(f"  - {col} {filter_config['operator']} {filter_config['value']}")
    else:
        print("Sem filtros aplicados")
    print(f"Critérios: Nulos >{null_threshold}%, Valor Único (=1), Zeros >{zero_threshold}%")
    print("=" * 70)
    # Monta e executa a query
    try:
        query, params = build_filtered_query(
            table_name=table_name,
            schema=schema,
            filters=filters
        )
        print(f"Query executada: {query}")
        if params:
            print(f"Parâmetros: {params}")
        df = query_sqlserver_safe(query, params)
    except Exception as e:
        print(f"Erro ao construir query: {e}")
        return None
    if df.empty:
        print("Não foi possível carregar dados da tabela")
        return None
    print(f"Analisando {len(df):,} registros, {len(df.columns)} colunas")
    columns_to_exclude = []
    exclusion_reasons = {}
    all_column_analysis = []
    print("\nANALISE DETALHADA DE TODAS AS COLUNAS:")
    print("-" * 70)
    for col in df.columns:
        col_data = df[col].dropna()
        total_data = df[col]
        reasons = []
        # Métricas básicas
        null_count = total_data.isnull().sum()
        null_percent = (null_count / len(total_data)) * 100
        unique_count = col_data.nunique() if len(col_data) > 0 else 0
        unique_percent = (unique_count / len(col_data)) * 100 if len(col_data) > 0 else 0
        # Critério 1: Muitos nulos
        if null_percent >= null_threshold:
            reasons.append(f"MUITOS NULOS ({null_percent:.1f}%)")
        # Critério 2: Variância (exatamente 1 valor único)
        if len(col_data) > 0 and unique_count == 1:
            reasons.append(f"VALOR ÚNICO ({col_data.iloc[0]})")
        # Critério 3: Muitos zeros (somente para colunas numéricas)
        zero_percent = 0
        if len(col_data) > 0 and pd.api.types.is_numeric_dtype(col_data):
            zero_count = (col_data == 0).sum()
            zero_percent = (zero_count / len(col_data)) * 100
            if zero_percent >= zero_threshold:
                reasons.append(f"MUITOS ZEROS ({zero_percent:.1f}%)")
        # Critério 4: Strings vazias (somente para colunas de texto)
        empty_percent = 0
        if len(col_data) > 0 and (pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data)):
            try:
                str_data = col_data.astype(str).str.strip()
                empty_count = (str_data == '').sum()
                empty_percent = (empty_count / len(str_data)) * 100
                if empty_percent >= zero_threshold:
                    reasons.append(f"STRINGS VAZIAS ({empty_percent:.1f}%)")
            except Exception:
                # Ignora erros na conversão para string
                pass
        # Decide ação e registra motivos
        if reasons:
            columns_to_exclude.append(col)
            exclusion_reasons[col] = reasons
            action = "EXCLUIR"
            reason_text = " | ".join(reasons)
        else:
            action = "MANTER"
            reason_text = f"{unique_count} únicos ({unique_percent:.1f}%), {null_percent:.1f}% nulos"
            if zero_percent > 0:
                reason_text += f", {zero_percent:.1f}% zeros"
            if empty_percent > 0:
                reason_text += f", {empty_percent:.1f}% vazias"
        # Guarda análise completa
        all_column_analysis.append({
            'Coluna': col,
            'Acao': action,
            'Nulos_Count': null_count,
            'Nulos_Percent': round(null_percent, 1),
            'Valores_Unicos': unique_count,
            'Variancia_Percent': round(unique_percent, 1),
            'Zeros_Percent': round(zero_percent, 1),
            'Vazias_Percent': round(empty_percent, 1),
            'Motivos': " | ".join(reasons) if reasons else "OK",
            'Tipo_Dados': str(df[col].dtype),
            'Dataframe': df
        })
        print(f"{col:<25} {action:<8} - {reason_text}")
    # Exibe resumo final
    print("\nRESUMO DA ANÁLISE:")
    print("-" * 50)
    print(f"Total de colunas analisadas: {len(df.columns)}")
    print(f"Colunas para MANTER: {len(df.columns) - len(columns_to_exclude)}")
    print(f"Colunas para EXCLUIR: {len(columns_to_exclude)}")
    if columns_to_exclude:
        print("\nLISTA COMPLETA DE EXCLUSÃO:")
        print("-" * 40)
        for i, col in enumerate(columns_to_exclude, 1):
            reasons_text = " | ".join(exclusion_reasons[col])
            print(f"{i:2d}. {col} → {reasons_text}")
        print("\nCOMANDO SQL PARA EXCLUSÃO:")
        print("-" * 40)
        print(f"-- Excluir {len(columns_to_exclude)} colunas da tabela {schema}.{table_name}")
        print(f"ALTER TABLE {schema}.{table_name}")
        print(f"DROP COLUMN {', '.join(columns_to_exclude)};")
        print("\n-- Ou criar nova tabela apenas com colunas úteis:")
        good_columns = [col for col in df.columns if col not in columns_to_exclude]
        columns_select = ', '.join(good_columns)
        print(f"SELECT {columns_select}")
        print(f"INTO {schema}.{table_name}_cleaned")
        print(f"FROM {schema}.{table_name}")
        if filters:
            where_parts = []
            for col_name, filter_config in filters.items():
                op = filter_config['operator']
                val = filter_config['value']
                if isinstance(val, str):
                    val = f"'{val}'"
                elif isinstance(val, (list, tuple)):
                    val = "(" + ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in val]) + ")"
                where_parts.append(f"{col_name} {op} {val}")
            print(f"WHERE {' AND '.join(where_parts)}")
        print(";")
    else:
        print("\nNENHUMA COLUNA PRECISA SER EXCLUÍDA!")
        print("Todas as colunas atendem aos critérios de qualidade.")
    # Gera arquivo Markdown
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"/home/suporte_amcom/Documentos/raphael-norris-ds/Projeto_IA_AMCOM/project_data_science/docs/data_quality/data_quality_{table_name}"
    try:
        markdown_content = generate_markdown_report(
            all_column_analysis, table_name, schema, filters,
            columns_to_exclude, exclusion_reasons, query, params
        )
        # Salva arquivo .md
        with open(f"{filename}.md", 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        print(f"\nRelatório Markdown salvo: {filename}.md")
    except Exception as e:
        print(f"Erro ao salvar Markdown: {e}")
    return {
        'total_columns': len(df.columns),
        'columns_to_exclude': columns_to_exclude,
        'columns_to_keep': [col for col in df.columns if col not in columns_to_exclude],
        'exclusion_reasons': exclusion_reasons,
        'all_analysis': all_column_analysis,
        'dataframe': df,
        'report_filename': filename,
        'query_executed': query,
        'query_params': params,
        'markdown_content': markdown_content
    }

def analyze_for_exclusion(table_name, schema='dbo', strict=False, filters=None):
    """
    Interface de alto nível para a identificação de colunas a serem excluídas.

    Args:
        table_name (str): Nome da tabela a ser analisada.
        schema (str): Schema da tabela. Default: 'dbo'.
        strict (bool): Se True, usa critérios mais rigorosos; caso contrário, usa critérios flexíveis. Default: False.
        filters (dict, opcional): Filtros a serem aplicados na consulta.

    Returns:
        dict: Resultado da função identify_columns_to_exclude com os critérios escolhidos.
    """
    if strict:
        return identify_columns_to_exclude(
            table_name, schema,
            filters=filters,
            null_threshold=70,
            zero_threshold=90
        )
    else:
        return identify_columns_to_exclude(
            table_name, schema,
            filters=filters,
            null_threshold=90,
            zero_threshold=80
        )

if __name__ == "__main__":
    print("ANALISADOR DE COLUNAS PARA EXCLUSÃO")
    print("=" * 50)
    conn, method = get_connection_sqlserver()
    if conn:
        print(f"Conexão OK usando {method}")
        conn.close()
        filters = {
            'DataCriacaoRegistro': {'operator': '>=', 'value': '2022-01-01'}
        }
        result = analyze_for_exclusion(
            table_name='Facas',
            schema='dbo',
            strict=False,
            filters=filters
        )
        if result:
            print("\nRESULTADO:")
            print(f"   Manter: {len(result['columns_to_keep'])} colunas")
            print(f"   Excluir: {len(result['columns_to_exclude'])} colunas")
            print(f"   Query executada: {result['query_executed']}")
            print(f"   Parâmetros: {result['query_params']}")
    else:
        print("ERRO: Não foi possível conectar")
