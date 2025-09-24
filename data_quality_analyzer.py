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

def identify_columns_to_exclude(table_name, schema='dbo', sample_size=5000, 
                               null_threshold=90, low_variance_threshold=1, 
                               zero_threshold=95):
    """
    Identifica colunas candidatas à exclusão baseado em critérios específicos
    
    Parâmetros:
    - null_threshold: % de nulos acima do qual a coluna é candidata à exclusão
    - low_variance_threshold: % de valores únicos abaixo do qual é baixa variância
    - zero_threshold: % de zeros acima do qual pode ser problemática
    """
    
    print(f"\nIDENTIFICANDO COLUNAS PARA EXCLUSÃO")
    print(f"Tabela: {schema}.{table_name}")
    print(f"Critérios: Nulos >{null_threshold}%, Variância <{low_variance_threshold}%, Zeros >{zero_threshold}%")
    print("=" * 70)
    
    # Carregar dados
    query = f"SELECT TOP {sample_size} * FROM {schema}.{table_name}"
    df = query_sqlserver_safe(query)
    
    if df.empty:
        print("Não foi possível carregar dados da tabela")
        return None
    
    print(f"Analisando {len(df):,} registros, {len(df.columns)} colunas")
    
    # Lista para armazenar todas as análises
    columns_to_exclude = []
    exclusion_reasons = {}
    all_column_analysis = []
    
    print(f"\nANALISE DETALHADA DE TODAS AS COLUNAS:")
    print("-" * 70)
    
    for col in df.columns:
        col_data = df[col].dropna()
        total_data = df[col]
        reasons = []
        
        # Calcular métricas básicas
        null_count = total_data.isnull().sum()
        null_percent = (null_count / len(total_data)) * 100
        unique_count = col_data.nunique() if len(col_data) > 0 else 0
        unique_percent = (unique_count / len(col_data)) * 100 if len(col_data) > 0 else 0
        
        # 1. ANÁLISE DE NULOS
        if null_percent >= null_threshold:
            reasons.append(f"MUITOS NULOS ({null_percent:.1f}%)")
        
        # 2. ANÁLISE DE VARIÂNCIA (apenas se tiver dados)
        if len(col_data) > 0:
            # Coluna com valor único
            if unique_count == 1:
                reasons.append(f"VALOR ÚNICO ({col_data.iloc[0]})")
            
            # Coluna com baixa variância
            elif unique_percent < low_variance_threshold:
                reasons.append(f"BAIXA VARIÂNCIA ({unique_count} valores únicos)")
        
        # 3. ANÁLISE DE ZEROS (para colunas numéricas)
        zero_percent = 0
        if len(col_data) > 0 and pd.api.types.is_numeric_dtype(col_data):
            zero_count = (col_data == 0).sum()
            zero_percent = (zero_count / len(col_data)) * 100
            
            if zero_percent >= zero_threshold:
                reasons.append(f"MUITOS ZEROS ({zero_percent:.1f}%)")
        
        # 4. ANÁLISE DE STRINGS VAZIAS
        empty_percent = 0
        if len(col_data) > 0 and (pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data)):
            try:
                str_data = col_data.astype(str).str.strip()
                empty_count = (str_data == '').sum()
                empty_percent = (empty_count / len(str_data)) * 100
                
                if empty_percent >= zero_threshold:
                    reasons.append(f"STRINGS VAZIAS ({empty_percent:.1f}%)")
            except:
                pass
        
        # Determinar ação e salvar análise completa
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
        
        # Armazenar análise completa
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
            'Tipo_Dados': str(df[col].dtype)
        })
        
        print(f"{col:<25} {action:<8} - {reason_text}")
    
    # RESUMO FINAL
    print(f"\nRESUMO DA ANÁLISE:")
    print("-" * 50)
    print(f"Total de colunas analisadas: {len(df.columns)}")
    print(f"Colunas para MANTER: {len(df.columns) - len(columns_to_exclude)}")
    print(f"Colunas para EXCLUIR: {len(columns_to_exclude)}")
    
    if columns_to_exclude:
        print(f"\nLISTA COMPLETA DE EXCLUSÃO:")
        print("-" * 40)
        for i, col in enumerate(columns_to_exclude, 1):
            reasons_text = " | ".join(exclusion_reasons[col])
            print(f"{i:2d}. {col} → {reasons_text}")
        
        # GERAR COMANDO SQL
        print(f"\nCOMANDO SQL PARA EXCLUSÃO:")
        print("-" * 40)
        print(f"-- Excluir {len(columns_to_exclude)} colunas da tabela {schema}.{table_name}")
        print(f"ALTER TABLE {schema}.{table_name}")
        print(f"DROP COLUMN {', '.join(columns_to_exclude)};")
        
        print(f"\n-- Ou criar nova tabela apenas com colunas úteis:")
        good_columns = [col for col in df.columns if col not in columns_to_exclude]
        columns_select = ', '.join(good_columns)
        print(f"SELECT {columns_select}")
        print(f"INTO {schema}.{table_name}_cleaned")
        print(f"FROM {schema}.{table_name};")
    
    else:
        print("\nNENHUMA COLUNA PRECISA SER EXCLUÍDA!")
        print("Todas as colunas atendem aos critérios de qualidade.")
    
    # Salvar relatório
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"data_quality_{table_name}_{timestamp}"
    
    # Salvar Excel com análise completa
    try:
        df_analysis = pd.DataFrame(all_column_analysis)
        
        with pd.ExcelWriter(f"{filename}.xlsx", engine='openpyxl') as writer:
            # Aba principal com análise de todas as colunas
            df_analysis.to_excel(writer, sheet_name='Analise_Completa', index=False)
            
            # Aba resumo
            summary_data = {
                'Métrica': [
                    'Total de Colunas',
                    'Colunas para Manter', 
                    'Colunas para Excluir',
                    'Percentual de Exclusão',
                    'Critério Nulos (%)',
                    'Critério Variância (%)',
                    'Critério Zeros (%)',
                    'Data/Hora Análise'
                ],
                'Valor': [
                    len(df.columns),
                    len(df.columns) - len(columns_to_exclude),
                    len(columns_to_exclude),
                    f"{(len(columns_to_exclude)/len(df.columns))*100:.1f}%",
                    f">{null_threshold}%",
                    f"<{low_variance_threshold}%", 
                    f">{zero_threshold}%",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Resumo', index=False)
            
            # Aba apenas com colunas para excluir
            if columns_to_exclude:
                df_exclude = df_analysis[df_analysis['Acao'] == 'EXCLUIR'].copy()
                df_exclude.to_excel(writer, sheet_name='Colunas_Excluir', index=False)
            
            # Aba apenas com colunas para manter
            df_keep = df_analysis[df_analysis['Acao'] == 'MANTER'].copy()
            df_keep.to_excel(writer, sheet_name='Colunas_Manter', index=False)
        
        print(f"\nRelatório Excel salvo: {filename}.xlsx")
        
    except Exception as e:
        print(f"Erro ao salvar Excel: {e}")
    
    # Salvar CSV simples
    try:
        df_analysis = pd.DataFrame(all_column_analysis)
        df_analysis.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
        print(f"Relatório CSV salvo: {filename}.csv")
    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
    
    # Retornar informações úteis
    return {
        'total_columns': len(df.columns),
        'columns_to_exclude': columns_to_exclude,
        'columns_to_keep': [col for col in df.columns if col not in columns_to_exclude],
        'exclusion_reasons': exclusion_reasons,
        'all_analysis': all_column_analysis,
        'dataframe': df,
        'report_filename': filename
    }

def analyze_for_exclusion(table_name, schema='dbo', sample_size=5000, strict=False):
    """
    Função simplificada para análise de exclusão
    
    strict=True: Critérios mais rigorosos
    strict=False: Critérios mais flexíveis
    """
    
    if strict:
        # Critérios rigorosos
        return identify_columns_to_exclude(
            table_name, schema, sample_size,
            null_threshold=70,      # 70% de nulos
            low_variance_threshold=2,   # <2% de valores únicos
            zero_threshold=90       # 90% de zeros
        )
    else:
        # Critérios mais flexíveis (padrão)
        return identify_columns_to_exclude(
            table_name, schema, sample_size,
            null_threshold=90,      # 90% de nulos
            low_variance_threshold=1,   # <1% de valores únicos  
            zero_threshold=95       # 95% de zeros
        )

if __name__ == "__main__":
    print("ANALISADOR DE COLUNAS PARA EXCLUSÃO")
    print("=" * 50)
    
    # Testar conexão
    conn, method = get_connection_sqlserver()
    if conn:
        print(f"✅ Conexão OK usando {method}")
        conn.close()
        
        # Análise para exclusão
        result = analyze_for_exclusion('Clientes', 'dbo', 3000, strict=False)
        
        if result:
            print(f"\n🎯 RESULTADO:")
            print(f"   Manter: {len(result['columns_to_keep'])} colunas")  
            print(f"   Excluir: {len(result['columns_to_exclude'])} colunas")
            
    else:
        print("❌ ERRO: Não foi possível conectar")
