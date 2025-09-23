import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Carregar .env
load_dotenv()

def get_connection_sqlserver():
    """Tenta diferentes métodos de conexão SQL Server"""
    
    server = os.getenv('SQLSERVER_HOST')
    database = os.getenv('SQLSERVER_DATABASE')
    username = os.getenv('SQLSERVER_USER')
    password = os.getenv('SQLSERVER_PASSWORD')
    
    # Método 1: pymssql (mais compatível com Linux)
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
    
    # Método 2: pyodbc com diferentes drivers
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
            except Exception as e:
                continue
                
    except ImportError:
        print("pyodbc não instalado")
    
    return None, None

def query_sqlserver_safe(query):
    """Executa query SQL Server com conexão robusta"""
    conn, method = get_connection_sqlserver()
    
    if not conn:
        print("ERRO: Não foi possível conectar ao SQL Server")
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erro na query: {e}")
        conn.close()
        return pd.DataFrame()

def analyze_data_quality_simple(table_name, schema='dbo', sample_size=5000):
    """Análise de qualidade de dados simplificada"""
    
    print(f"\nANALISE DE QUALIDADE DE DADOS")
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tabela: {schema}.{table_name}")
    print("=" * 60)
    
    # Carregar dados
    query = f"SELECT TOP {sample_size} * FROM {schema}.{table_name}"
    df = query_sqlserver_safe(query)
    
    if df.empty:
        print("Não foi possível carregar dados da tabela")
        return None
    
    print(f"Carregados {len(df):,} registros para análise")
    
    # Análise de nulos
    print(f"\n1. ANALISE DE VALORES NULOS")
    print("-" * 40)
    
    null_analysis = []
    total_rows = len(df)
    
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_percent = (null_count / total_rows) * 100
        
        if null_percent == 0:
            level = "OK"
        elif null_percent < 10:
            level = "BAIXO"
        elif null_percent < 30:
            level = "MEDIO"
        else:
            level = "ALTO"
        
        null_analysis.append({
            'Coluna': col,
            'Nulos': null_count,
            'Percent': round(null_percent, 1),
            'Level': level
        })
    
    # Mostrar apenas colunas com problemas
    problem_nulls = [x for x in null_analysis if x['Percent'] > 0]
    if problem_nulls:
        for item in sorted(problem_nulls, key=lambda x: x['Percent'], reverse=True)[:10]:
            print(f"{item['Coluna']:<20} {item['Nulos']:>6} ({item['Percent']:>5.1f}%) - {item['Level']}")
    else:
        print("Nenhuma coluna com valores nulos encontrada")
    
    # Análise de variabilidade
    print(f"\n2. ANALISE DE VARIABILIDADE")
    print("-" * 40)
    
    var_problems = []
    
    for col in df.columns:
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue
        
        unique_count = col_data.nunique()
        unique_percent = (unique_count / len(col_data)) * 100
        
        # Problemas de variabilidade
        if unique_count == 1:
            var_problems.append(f"ZERO VARIACAO - {col}: Todos valores iguais a '{col_data.iloc[0]}'")
        elif unique_percent < 1:
            var_problems.append(f"BAIXA VARIACAO - {col}: Apenas {unique_count} valores únicos")
        
        # Para colunas numéricas, verificar zeros
        if pd.api.types.is_numeric_dtype(col_data):
            zero_percent = (col_data == 0).sum() / len(col_data) * 100
            if zero_percent > 70:
                var_problems.append(f"MUITOS ZEROS - {col}: {zero_percent:.1f}% são zeros")
    
    if var_problems:
        for problem in var_problems[:10]:  # Mostrar apenas os 10 primeiros
            print(problem)
    else:
        print("Nenhum problema de variabilidade crítico encontrado")
    
    # Análise de padrões
    print(f"\n3. PADROES PROBLEMATICOS")
    print("-" * 40)
    
    pattern_problems = []
    
    for col in df.columns:
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue
        
        # Para strings, verificar vazias
        if pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
            str_data = col_data.astype(str).str.strip()
            empty_percent = (str_data == '').sum() / len(str_data) * 100
            
            if empty_percent > 50:
                pattern_problems.append(f"STRINGS VAZIAS - {col}: {empty_percent:.1f}% vazias")
            
            # Verificar dominância de um valor
            if len(str_data) > 1:
                top_value_percent = str_data.value_counts().iloc[0] / len(str_data) * 100
                if top_value_percent > 95:
                    top_value = str_data.value_counts().index[0]
                    pattern_problems.append(f"VALOR DOMINANTE - {col}: {top_value_percent:.1f}% são '{top_value}'")
    
    if pattern_problems:
        for problem in pattern_problems:
            print(problem)
    else:
        print("Nenhum padrão problemático identificado")
    
    # Resumo
    print(f"\n4. RESUMO GERAL")
    print("-" * 40)
    
    total_cols = len(df.columns)
    null_problems = len([x for x in null_analysis if x['Percent'] > 10])
    var_problem_count = len(var_problems)
    pattern_problem_count = len(pattern_problems)
    
    total_problems = null_problems + var_problem_count + pattern_problem_count
    quality_score = max(0, ((total_cols - total_problems) / total_cols) * 100)
    
    print(f"Total de colunas: {total_cols}")
    print(f"Colunas com +10% nulos: {null_problems}")
    print(f"Problemas de variabilidade: {var_problem_count}")
    print(f"Problemas de padrões: {pattern_problem_count}")
    print(f"Score de qualidade: {quality_score:.1f}%")
    
    if quality_score >= 80:
        print("STATUS: EXCELENTE")
    elif quality_score >= 60:
        print("STATUS: BOA")
    elif quality_score >= 40:
        print("STATUS: REGULAR")
    else:
        print("STATUS: RUIM")
    
    return df

# Função principal
def analyze_table(table_name, schema='dbo', sample_size=5000):
    """Função principal para análise de qualidade"""
    return analyze_data_quality_simple(table_name, schema, sample_size)

if __name__ == "__main__":
    # Teste
    print("TESTE DE CONEXAO E ANALISE")
    print("=" * 50)
    
    # Testar conexão primeiro
    conn, method = get_connection_sqlserver()
    if conn:
        print(f"Conexão OK usando {method}")
        conn.close()
        
        # Fazer análise
        result = analyze_table('Clientes', 'dbo', 3000)
    else:
        print("ERRO: Não foi possível conectar")
        print("\nSOLUCOES:")
        print("1. Instalar pymssql: pip install pymssql")
        print("2. Verificar .env com credenciais")
        print("3. Verificar conectividade de rede")
