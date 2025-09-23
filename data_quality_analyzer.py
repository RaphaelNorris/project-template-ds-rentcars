import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
from dotenv import load_dotenv
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Carregar .env
load_dotenv()

class DataQualityAnalyzer:
    def __init__(self, connection_type='sqlserver'):
        """
        Inicializa o analisador de qualidade de dados
        
        Args:
            connection_type (str): 'sqlserver' ou 'oracle'
        """
        self.connection_type = connection_type
        self.engine = self._get_engine()
        
    def _get_engine(self):
        """Cria engine de conexão baseado no tipo"""
        try:
            if self.connection_type == 'sqlserver':
                server = os.getenv('SQLSERVER_HOST')
                database = os.getenv('SQLSERVER_DATABASE')
                username = os.getenv('SQLSERVER_USER')
                password = os.getenv('SQLSERVER_PASSWORD')
                
                conn_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server&TrustServerCertificate=yes"
                
            elif self.connection_type == 'oracle':
                user = os.getenv('ORACLE_RAW_USER')
                password = os.getenv('ORACLE_RAW_PASSWORD')
                host = os.getenv('ORACLE_HOST')
                port = os.getenv('ORACLE_PORT')
                service = os.getenv('ORACLE_SERVICE_NAME')
                
                conn_string = f"oracle+oracledb://{user}:{password}@{host}:{port}/{service}"
            
            return create_engine(conn_string)
            
        except Exception as e:
            print(f"Erro ao criar engine: {e}")
            return None
    
    def get_table_sample(self, table_name, sample_size=10000, schema=None):
        """
        Obtém amostra da tabela para análise
        
        Args:
            table_name (str): Nome da tabela
            sample_size (int): Tamanho da amostra
            schema (str): Schema da tabela (opcional)
        """
        try:
            if schema:
                full_table_name = f"{schema}.{table_name}"
            else:
                full_table_name = table_name
            
            if self.connection_type == 'sqlserver':
                query = f"SELECT TOP {sample_size} * FROM {full_table_name}"
            else:  # oracle
                query = f"SELECT * FROM {full_table_name} WHERE ROWNUM <= {sample_size}"
            
            df = pd.read_sql(query, self.engine)
            print(f"✅ Carregada amostra de {len(df):,} registros da tabela {full_table_name}")
            return df
            
        except Exception as e:
            print(f"❌ Erro ao carregar tabela {table_name}: {e}")
            return pd.DataFrame()
    
    def analyze_nulls(self, df, table_name):
        """Análise de valores nulos"""
        print(f"\n{'='*70}")
        print(f"ANÁLISE DE VALORES NULOS - {table_name}")
        print('='*70)
        
        null_analysis = []
        total_rows = len(df)
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_percent = (null_count / total_rows) * 100
            
            # Classificação do nível de nulos
            if null_percent == 0:
                null_level = "✅ SEM NULOS"
            elif null_percent < 5:
                null_level = "🟢 BAIXO"
            elif null_percent < 20:
                null_level = "🟡 MÉDIO"
            elif null_percent < 50:
                null_level = "🟠 ALTO"
            else:
                null_level = "🔴 CRÍTICO"
            
            null_analysis.append({
                'Coluna': col,
                'Tipo': str(df[col].dtype),
                'Nulos': null_count,
                'Percentual_Nulo': round(null_percent, 2),
                'Nivel': null_level
            })
        
        null_df = pd.DataFrame(null_analysis)
        null_df = null_df.sort_values('Percentual_Nulo', ascending=False)
        
        print(null_df.to_string(index=False))
        return null_df
    
    def analyze_variability(self, df, table_name):
        """Análise de variabilidade dos dados"""
        print(f"\n{'='*70}")
        print(f"ANÁLISE DE VARIABILIDADE - {table_name}")
        print('='*70)
        
        variability_analysis = []
        total_rows = len(df)
        
        for col in df.columns:
            col_data = df[col].dropna()  # Remove nulos para análise
            
            if len(col_data) == 0:
                continue
            
            unique_count = col_data.nunique()
            unique_percent = (unique_count / len(col_data)) * 100
            
            # Análise específica por tipo
            analysis_result = self._analyze_column_content(col_data, col)
            
            # Classificação de variabilidade
            if unique_count == 1:
                variability_level = "🔴 ZERO VARIAÇÃO"
            elif unique_percent < 1:
                variability_level = "🟠 BAIXA VARIAÇÃO"
            elif unique_percent < 10:
                variability_level = "🟡 MÉDIA VARIAÇÃO"
            else:
                variability_level = "✅ BOA VARIAÇÃO"
            
            variability_analysis.append({
                'Coluna': col,
                'Tipo': str(df[col].dtype),
                'Valores_Unicos': unique_count,
                'Percentual_Unico': round(unique_percent, 2),
                'Nivel_Variabilidade': variability_level,
                'Detalhes': analysis_result
            })
        
        var_df = pd.DataFrame(variability_analysis)
        var_df = var_df.sort_values('Percentual_Unico', ascending=True)
        
        print(var_df.to_string(index=False))
        return var_df
    
    def _analyze_column_content(self, col_data, col_name):
        """Análise detalhada do conteúdo da coluna"""
        try:
            # Análise para colunas numéricas
            if pd.api.types.is_numeric_dtype(col_data):
                zero_count = (col_data == 0).sum()
                zero_percent = (zero_count / len(col_data)) * 100
                
                if zero_percent > 90:
                    return f"⚠️ {zero_percent:.1f}% são zeros"
                elif zero_percent > 50:
                    return f"⚠️ {zero_percent:.1f}% são zeros"
                else:
                    return f"Min: {col_data.min()}, Max: {col_data.max()}"
            
            # Análise para colunas de texto
            elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
                empty_count = (col_data.astype(str).str.strip() == '').sum()
                empty_percent = (empty_count / len(col_data)) * 100
                
                if empty_percent > 50:
                    return f"⚠️ {empty_percent:.1f}% são strings vazias"
                
                # Verificar valores mais comuns
                top_values = col_data.value_counts().head(3)
                top_value_percent = (top_values.iloc[0] / len(col_data)) * 100
                
                if top_value_percent > 80:
                    return f"⚠️ {top_value_percent:.1f}% são '{top_values.index[0]}'"
                else:
                    return f"Top: {top_values.index[0]} ({top_value_percent:.1f}%)"
            
            # Análise para colunas de data
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                return f"De {col_data.min()} até {col_data.max()}"
            
            else:
                return "Tipo não identificado"
                
        except Exception as e:
            return f"Erro na análise: {str(e)[:30]}"
    
    def analyze_data_patterns(self, df, table_name):
        """Análise de padrões problemáticos"""
        print(f"\n{'='*70}")
        print(f"ANÁLISE DE PADRÕES PROBLEMÁTICOS - {table_name}")
        print('='*70)
        
        problems = []
        
        for col in df.columns:
            col_data = df[col].dropna()
            
            if len(col_data) == 0:
                problems.append(f"🔴 {col}: Coluna completamente vazia")
                continue
            
            # Problema 1: Todas as linhas iguais
            if col_data.nunique() == 1:
                value = col_data.iloc[0]
                problems.append(f"🔴 {col}: Todos os valores são '{value}'")
            
            # Problema 2: Muitos zeros (para numérico)
            elif pd.api.types.is_numeric_dtype(col_data):
                zero_percent = (col_data == 0).sum() / len(col_data) * 100
                if zero_percent > 80:
                    problems.append(f"🟠 {col}: {zero_percent:.1f}% dos valores são zero")
            
            # Problema 3: Strings vazias ou muito repetitivas
            elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
                str_data = col_data.astype(str).str.strip()
                empty_percent = (str_data == '').sum() / len(str_data) * 100
                
                if empty_percent > 70:
                    problems.append(f"🟡 {col}: {empty_percent:.1f}% são strings vazias")
                
                # Verificar se há valor muito dominante
                top_value_percent = str_data.value_counts().iloc[0] / len(str_data) * 100
                if top_value_percent > 90:
                    top_value = str_data.value_counts().index[0]
                    problems.append(f"🟡 {col}: {top_value_percent:.1f}% são '{top_value}'")
        
        if problems:
            for problem in problems:
                print(problem)
        else:
            print("✅ Nenhum padrão problemático identificado!")
        
        return problems
    
    def generate_summary_report(self, df, table_name):
        """Gera relatório resumido"""
        print(f"\n{'='*70}")
        print(f"RELATÓRIO RESUMIDO - {table_name}")
        print('='*70)
        
        total_rows = len(df)
        total_cols = len(df.columns)
        
        # Contadores de problemas
        zero_var_cols = 0
        high_null_cols = 0
        problematic_cols = 0
        
        for col in df.columns:
            # Variação zero
            if df[col].nunique() <= 1:
                zero_var_cols += 1
            
            # Muitos nulos
            null_percent = (df[col].isnull().sum() / total_rows) * 100
            if null_percent > 50:
                high_null_cols += 1
            
            # Problemas diversos
            col_data = df[col].dropna()
            if len(col_data) > 0:
                if pd.api.types.is_numeric_dtype(col_data):
                    zero_percent = (col_data == 0).sum() / len(col_data) * 100
                    if zero_percent > 80:
                        problematic_cols += 1
        
        print(f"📊 Total de registros: {total_rows:,}")
        print(f"📊 Total de colunas: {total_cols}")
        print(f"🔴 Colunas sem variação: {zero_var_cols}")
        print(f"🟠 Colunas com +50% nulos: {high_null_cols}")
        print(f"🟡 Colunas problemáticas: {problematic_cols}")
        
        # Qualidade geral
        quality_score = ((total_cols - zero_var_cols - high_null_cols - problematic_cols) / total_cols) * 100
        
        if quality_score >= 80:
            quality_level = "✅ EXCELENTE"
        elif quality_score >= 60:
            quality_level = "🟢 BOA"
        elif quality_score >= 40:
            quality_level = "🟡 REGULAR"
        else:
            quality_level = "🔴 RUIM"
        
        print(f"🎯 Qualidade geral: {quality_score:.1f}% - {quality_level}")
    
    def analyze_table(self, table_name, schema=None, sample_size=10000):
        """
        Análise completa de uma tabela
        
        Args:
            table_name (str): Nome da tabela
            schema (str): Schema (opcional)
            sample_size (int): Tamanho da amostra
        """
        print(f"\n🔍 INICIANDO ANÁLISE DE QUALIDADE DE DADOS")
        print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏢 Tabela: {schema}.{table_name}" if schema else f"🏢 Tabela: {table_name}")
        
        # Carregar dados
        df = self.get_table_sample(table_name, sample_size, schema)
        
        if df.empty:
            print("❌ Não foi possível carregar dados da tabela")
            return
        
        # Executar análises
        null_analysis = self.analyze_nulls(df, table_name)
        variability_analysis = self.analyze_variability(df, table_name)
        patterns = self.analyze_data_patterns(df, table_name)
        self.generate_summary_report(df, table_name)
        
        # Retornar resultados para uso posterior
        return {
            'null_analysis': null_analysis,
            'variability_analysis': variability_analysis,
            'patterns': patterns,
            'sample_data': df
        }

# =============================================================================
# FUNÇÃO DE CONVENIÊNCIA
# =============================================================================

def analyze_table_quality(table_name, connection_type='sqlserver', schema=None, sample_size=10000):
    """
    Função simples para analisar qualidade de uma tabela
    
    Args:
        table_name (str): Nome da tabela
        connection_type (str): 'sqlserver' ou 'oracle'
        schema (str): Schema (opcional)
        sample_size (int): Tamanho da amostra
    """
    analyzer = DataQualityAnalyzer(connection_type)
    return analyzer.analyze_table(table_name, schema, sample_size)

# =============================================================================
# EXEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    # Exemplo SQL Server
    print("🧪 EXEMPLO DE ANÁLISE - SQL SERVER")
    result = analyze_table_quality('Clientes', 'sqlserver', 'dbo', 5000)
    
    # Exemplo Oracle (descomente para usar)
    # print("\n🧪 EXEMPLO DE ANÁLISE - ORACLE")
    # result = analyze_table_quality('TABELA_EXEMPLO', 'oracle', 'TRIMBOX_RAW', 5000)
