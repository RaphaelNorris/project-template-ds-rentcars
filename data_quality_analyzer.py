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

def get_table_row_count(table_name, schema='dbo'):
    """Obtém o número total de linhas da tabela"""
    query = f"SELECT COUNT(*) as total_rows FROM {schema}.{table_name}"
    result = query_sqlserver_safe(query)
    return result.iloc[0]['total_rows'] if not result.empty else None

def detect_date_columns(df):
    """Detecta colunas que podem ser usadas como filtro de data"""
    date_columns = []
    
    for col in df.columns:
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue
            
        # Verificar se é datetime
        if pd.api.types.is_datetime64_any_dtype(col_data):
            date_columns.append(col)
            continue
            
        # Tentar converter strings para datetime
        if pd.api.types.is_object_dtype(col_data):
            try:
                # Tentar converter uma amostra pequena primeiro
                sample = col_data.head(min(100, len(col_data)))
                pd.to_datetime(sample, errors='raise')
                date_columns.append(col)
            except:
                pass
    
    return date_columns

def analyze_full_table_variance(table_name, schema='dbo'):
    """Analisa variância considerando a tabela completa para colunas críticas"""
    
    # Query para verificar variabilidade em toda a tabela
    variance_query = f"""
    SELECT 
        COLUMN_NAME,
        DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    
    columns_info = query_sqlserver_safe(variance_query)
    if columns_info.empty:
        return {}
    
    variance_results = {}
    
    # Para cada coluna, verificar se tem apenas um valor único
    for _, col_info in columns_info.iterrows():
        col_name = col_info['COLUMN_NAME']
        
        # Query para contar valores distintos (excluindo nulos)
        distinct_query = f"""
        SELECT COUNT(DISTINCT {col_name}) as distinct_count
        FROM {schema}.{table_name}
        WHERE {col_name} IS NOT NULL
        """
        
        try:
            result = query_sqlserver_safe(distinct_query)
            if not result.empty:
                distinct_count = result.iloc[0]['distinct_count']
                variance_results[col_name] = {
                    'distinct_count': distinct_count,
                    'zero_variance': distinct_count <= 1
                }
        except Exception as e:
            print(f"Erro ao analisar variância da coluna {col_name}: {e}")
            variance_results[col_name] = {
                'distinct_count': None,
                'zero_variance': False
            }
    
    return variance_results

def identify_columns_to_exclude(table_name, schema='dbo', sample_size=5000, 
                               null_threshold=90, zero_threshold=95, 
                               analyze_full_table=True):
    """
    Identifica colunas candidatas à exclusão baseado em critérios específicos
    
    Parâmetros:
    - null_threshold: % de nulos acima do qual a coluna é candidata à exclusão
    - zero_threshold: % de zeros acima do qual pode ser problemática
    - analyze_full_table: Se True, analisa variância na tabela completa
    """
    
    print(f"\nIDENTIFICANDO COLUNAS PARA EXCLUSÃO")
    print(f"Tabela: {schema}.{table_name}")
    print(f"Critérios: Nulos >{null_threshold}%, Zeros >{zero_threshold}%, Variância = 0 (valor único)")
    print("=" * 70)
    
    # Obter informações da tabela completa
    total_rows = get_table_row_count(table_name, schema)
    if total_rows:
        print(f"Total de registros na tabela: {total_rows:,}")
    
    # Analisar variância na tabela completa se solicitado
    full_table_variance = {}
    if analyze_full_table:
        print("Analisando variância na tabela completa...")
        full_table_variance = analyze_full_table_variance(table_name, schema)
    
    # Carregar amostra dos dados
    query = f"SELECT TOP {sample_size} * FROM {schema}.{table_name}"
    df = query_sqlserver_safe(query)
    
    if df.empty:
        print("Não foi possível carregar dados da tabela")
        return None
    
    print(f"Analisando amostra de {len(df):,} registros, {len(df.columns)} colunas")
    
    # Detectar colunas de data
    date_columns = detect_date_columns(df)
    if date_columns:
        print(f"Colunas de data detectadas: {', '.join(date_columns)}")
        print("SUGESTÃO: Use uma dessas colunas para filtros temporais em análises futuras")
    else:
        print("Nenhuma coluna de data detectada")
    
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
        
        # Usar análise da tabela completa se disponível
        if col in full_table_variance:
            full_unique_count = full_table_variance[col]['distinct_count']
            is_zero_variance_full = full_table_variance[col]['zero_variance']
        else:
            full_unique_count = unique_count
            is_zero_variance_full = unique_count <= 1
        
        # 1. ANÁLISE DE NULOS
        if null_percent >= null_threshold:
            reasons.append(f"MUITOS NULOS ({null_percent:.1f}%)")
        
        # 2. ANÁLISE DE VARIÂNCIA - APENAS ZERO VARIÂNCIA (valor único)
        if len(col_data) > 0 and is_zero_variance_full:
            if full_unique_count == 0:
                reasons.append("SEM DADOS VÁLIDOS")
            elif full_unique_count == 1:
                sample_value = col_data.iloc[0] if len(col_data) > 0 else "N/A"
                reasons.append(f"VALOR ÚNICO ({sample_value})")
        
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
            reason_text = f"{full_unique_count if full_unique_count else unique_count} únicos"
            if null_percent > 0:
                reason_text += f", {null_percent:.1f}% nulos"
            if zero_percent > 0:
                reason_text += f", {zero_percent:.1f}% zeros"
            if empty_percent > 0:
                reason_text += f", {empty_percent:.1f}% vazias"
        
        # Marcar colunas de data
        col_display = col
        if col in date_columns:
            col_display += " [DATA]"
        
        # Armazenar análise completa
        all_column_analysis.append({
            'Coluna': col,
            'Acao': action,
            'Nulos_Count': null_count,
            'Nulos_Percent': round(null_percent, 1),
            'Valores_Unicos_Amostra': unique_count,
            'Valores_Unicos_Tabela': full_unique_count if full_unique_count else unique_count,
            'Zeros_Percent': round(zero_percent, 1),
            'Vazias_Percent': round(empty_percent, 1),
            'Motivos': " | ".join(reasons) if reasons else "OK",
            'Tipo_Dados': str(df[col].dtype),
            'Coluna_Data': col in date_columns
        })
        
        print(f"{col_display:<30} {action:<8} - {reason_text}")
    
    # RESUMO FINAL
    print(f"\nRESUMO DA ANÁLISE:")
    print("-" * 50)
    print(f"Total de colunas analisadas: {len(df.columns)}")
    print(f"Colunas de data detectadas: {len(date_columns)}")
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
        
        with pd.ExcelWriter(f"{filename}.xlsx", engine='xlsxwriter') as writer:
            # Aba principal com análise de todas as colunas
            df_analysis.to_excel(writer, sheet_name='Analise_Completa', index=False)
            
            # Aba resumo
            summary_data = {
                'Métrica': [
                    'Total de Registros (Tabela)',
                    'Registros Analisados (Amostra)',
                    'Total de Colunas',
                    'Colunas de Data',
                    'Colunas para Manter', 
                    'Colunas para Excluir',
                    'Percentual de Exclusão',
                    'Critério Nulos (%)',
                    'Critério Zeros (%)',
                    'Critério Variância',
                    'Data/Hora Análise'
                ],
                'Valor': [
                    total_rows if total_rows else 'N/A',
                    len(df),
                    len(df.columns),
                    len(date_columns),
                    len(df.columns) - len(columns_to_exclude),
                    len(columns_to_exclude),
                    f"{(len(columns_to_exclude)/len(df.columns))*100:.1f}%",
                    f">{null_threshold}%",
                    f">{zero_threshold}%",
                    "Apenas valor único",
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
            
            # Aba com colunas de data (se existirem)
            if date_columns:
                df_dates = df_analysis[df_analysis['Coluna_Data'] == True].copy()
                df_dates.to_excel(writer, sheet_name='Colunas_Data', index=False)
        
        print(f"\nRelatório Excel salvo: {filename}.xlsx")
        
    except Exception as e:
        print(f"Erro ao salvar Excel: {e}")
        print("Tentando instalar xlsxwriter: pip install xlsxwriter")
    
    # Salvar CSV simples
    try:
        df_analysis = pd.DataFrame(all_column_analysis)
        df_analysis.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
        print(f"Relatório CSV salvo: {filename}.csv")
    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
    
    # Retornar informações úteis
    return {
        'total_rows': total_rows,
        'total_columns': len(df.columns),
        'date_columns': date_columns,
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
        print(f"Conexão OK usando {method}")
        conn.close()
        
        # Análise para exclusão
        result = analyze_for_exclusion('Clientes', 'dbo', 3000, strict=False)
        
        if result:
            print(f"\nRESULTADO:")
            print(f"   Manter: {len(result['columns_to_keep'])} colunas")  
            print(f"   Excluir: {len(result['columns_to_exclude'])} colunas")
            print(f"   Relatório salvo: {result['report_filename']}.xlsx/.csv")
            
    else:
        print("ERRO: Não foi possível conectar")
