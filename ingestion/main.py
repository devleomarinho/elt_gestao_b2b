# -*- coding: utf-8 -*-
import os
import json
import logging
import sys
import traceback
import re
import unicodedata
from datetime import datetime, timezone
from collections import defaultdict

import google.auth
import gspread
import pandas as pd
from google.cloud import bigquery

# ====================================================================================
# Configuração de Logging (Padrão Cloud Logging)
# ====================================================================================
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ====================================================================================
# Configurações Globais
# ====================================================================================

# Escopos são necessários para gerar o token de acesso correto para Sheets/Drive
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
]

# Configuração das Planilhas de Origem
SHEETS_CONFIG = [
    {
        "sheet_name": "ALCANCE_CONSOLIDADO_LINKEDIN",
        "sheet_id": "1GzgIKRsBhgxHvo709OpdElHeK0OYdz_zg2hEoG3XnJQ",
        "tabs": ["ALCANCE_CONSOLIDADO_LINKEDIN_RAW"]
    },
    {
        "sheet_name": "ALCANCE_CONSOLIDADO_CM",
        "sheet_id": "1XWHOlxJaqWFrkB6n7lq3HLilZnC2S-s5H-nmSelMpYk",
        "tabs": ["ALCANCE_CONSOLIDADO_CM_RAW"]
    },
    {
        "sheet_name": "ALCANCE_CONSOLIDADO_META",
        "sheet_id": "1hTzFKovdUxpGiwnHmGgvmNa25lepS7PV2mx3slFSAmw",
        "tabs": ["ALCANCE_CONSOLIDADO_META_RAW"]
    },
    {
        "sheet_name": "ALCANCE_CONSOLIDADO_TIKTOK",
        "sheet_id": "1v0uNwCr10lSdod7qoSppOd3m3Edpk--OaYPxenv0cpw",
        "tabs": ["ALCANCE_CONSOLIDADO_TIKTOK_RAW"]
    }
]

# Definições do BigQuery
DATASET_ID = "staging" # Nome do dataset de destino
LOCATION = "southamerica-east1" # Região do BigQuery

# ====================================================================================
# Autenticação (ADC - Application Default Credentials)
# ====================================================================================
def get_authenticated_clients():
    """
    Autentica usando a identidade do ambiente (Service Account no Cloud Run 
    ou Credenciais de Usuário localmente).
    """
    logging.info("Iniciando autenticação (ADC)...")
    
    try:
        # google.auth.default descobre as credenciais automaticamente
        creds, project_id = google.auth.default(scopes=SCOPES)
        
        # Cliente do Google Sheets
        gc = gspread.authorize(creds)
        
        # Cliente do BigQuery
        bq_client = bigquery.Client(credentials=creds, project=project_id)
        
        logging.info(f"Autenticado no projeto: {project_id}")
        return gc, bq_client, project_id
        
    except Exception as e:
        logging.critical(f"!!! Falha fatal na autenticação: {e}")
        raise

# ====================================================================================
# Funções Utilitárias de Tratamento de Strings e Colunas
# ====================================================================================
def sanitize_text(text):
    """Remove acentos e caracteres especiais de uma string."""
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return text

def sanitize_tablename(tab_name):
    """Normaliza nome da tabela para o padrão do BigQuery."""
    name = sanitize_text(tab_name)
    name = re.sub(r"[\W]+", "_", name)
    return name.strip("_").lower()

def sanitize_bq_column_name(column_name):
    """Normaliza nome da coluna para o padrão do BigQuery."""
    name = sanitize_text(column_name)
    name = re.sub(r'[^\w_]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    # Se começar com número ou for vazio, adiciona prefixo
    if not name or name[0].isdigit():
        name = 'col_' + name
    return name[:300].lower()

def sanitize_dataframe_columns(df):
    """Garante que todas as colunas do DF sejam nomes válidos e únicos no BQ."""
    new_columns = [sanitize_bq_column_name(col) for col in df.columns]
    seen = defaultdict(int)
    final_columns = []
    
    for col in new_columns:
        if col in final_columns:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            final_columns.append(col)
            
    df.columns = final_columns
    return df

def df_all_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte todo o DataFrame para String para garantir ingestão (ELT).
    Tratamento de tipos será feito posteriormente no dbt.
    """
    out = df.copy()
    for col in out.columns:
        # Converte para string, mantendo None/NaN como None
        out[col] = out[col].astype(str).replace({'nan': None, 'None': None, '<NA>': None})
    return out

def to_snake_case(text: str) -> str:
    text = sanitize_text(text).lower()
    text = re.sub(r'[\W]+', '_', text)
    return text.strip('_')

# ====================================================================================
# Leitura do Google Sheets
# ====================================================================================
def safe_get_records(worksheet):
    """Lê os dados da aba de forma segura."""
    try:
        # Lê os valores formatados (como o usuário vê no Sheets)
        values = worksheet.get_all_values(value_render_option='FORMATTED_VALUE')
        
        if not values:
            return pd.DataFrame()
            
        header, rows = values[0], values[1:]
        
        # Garante cabeçalhos únicos caso o usuário tenha colunas repetidas
        header = [f"{col}_{i}" if header.count(col) > 1 else col for i, col in enumerate(header)]
        
        df = pd.DataFrame(rows, columns=header)
        # Substitui strings vazias por None
        df = df.replace({"": None})
        return df
        
    except Exception as e:
        logging.error(f"Erro ao ler planilha: {e}")
        return pd.DataFrame()

# ====================================================================================
# Operações BigQuery
# ====================================================================================
def ensure_dataset(client: bigquery.Client, project_id: str):
    """Verifica se o dataset existe, senão cria."""
    dataset_ref = f"{project_id}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset '{DATASET_ID}' encontrado.")
    except Exception:
        logging.warning(f"Dataset '{DATASET_ID}' não encontrado. Criando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset, timeout=30)
        logging.info(f"Dataset '{DATASET_ID}' criado com sucesso.")

def upload_to_bq_landing(client: bigquery.Client, df: pd.DataFrame, table_name: str, project_id: str):
    """Faz o upload do DataFrame para o BigQuery (Full Refresh / Truncate)."""
    try:
        # 1. Higienização de nomes de colunas
        df_sanitized = sanitize_dataframe_columns(df.copy())
        
        # 2. Conversão forçada para String (Schema-on-read logic)
        df_prepared = df_all_to_string(df_sanitized)
        
        # 3. Definição do Schema (Tudo String)
        bq_schema = [bigquery.SchemaField(col, "STRING") for col in df_prepared.columns]

        full_table_id = f"{project_id}.{DATASET_ID}.{table_name}"
        
        logging.info(f"Iniciando upload de {len(df_prepared)} linhas para {full_table_id}...")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Sobrescreve tabela
            schema=bq_schema
        )

        load_job = client.load_table_from_dataframe(
            df_prepared, full_table_id, job_config=job_config
        )
        
        load_job.result() # Aguarda conclusão
        
        table = client.get_table(full_table_id)
        logging.info(f"!!! Sucesso: Tabela '{table_name}' atualizada com {table.num_rows} registros.")
        return True
        
    except Exception:
        logging.error(f"!!! Erro CRÍTICO no upload para a tabela {table_name}.")
        traceback.print_exc()
        raise

# ====================================================================================
# Pipeline Principal
# ====================================================================================
def run_pipeline():
    logging.info("▶️ Iniciando Pipeline de Ingestão (Sheets -> BigQuery)...")
    
    # 1. Configuração e Autenticação
    try:
        gc, bq_client, project_id = get_authenticated_clients()
        ensure_dataset(bq_client, project_id)
    except Exception:
        logging.error("Falha na inicialização do ambiente. Abortando.")
        sys.exit(1)
        
    # 2. Iteração sobre as fontes
    execution_stats = {"success": 0, "error": 0, "skipped": 0}
    
    for config in SHEETS_CONFIG:
        sheet_name = config["sheet_name"]
        sheet_id = config["sheet_id"]
        
        for tab in config["tabs"]:
            logging.info(f"--- Processando: {sheet_name} | Aba: {tab} ---")
            
            try:
                # Abrir planilha
                try:
                    sheet = gc.open_by_key(sheet_id)
                    worksheet = sheet.worksheet(tab)
                except Exception as e:
                    logging.error(f"Não foi possível acessar a aba '{tab}': {e}")
                    execution_stats["error"] += 1
                    continue
                
                # Extrair dados
                df = safe_get_records(worksheet)
                df = df.dropna(how='all') # Remove linhas completamente vazias
                
                if df.empty:
                    logging.warning(f"Aba '{tab}' está vazia. Pulando.")
                    execution_stats["skipped"] += 1
                    continue
                
                # Adicionar metadados de engenharia
                df['_source_sheet'] = to_snake_case(sheet_name)
                df['_ingested_at'] = datetime.now(timezone.utc).isoformat()
                
                # Definir nome da tabela de destino
                table_name = sanitize_tablename(tab)
                
                # Carga no BQ
                upload_to_bq_landing(bq_client, df, table_name, project_id)
                execution_stats["success"] += 1
                
            except Exception as e:
                logging.error(f"Falha não tratada no processamento de '{tab}': {e}")
                execution_stats["error"] += 1
                traceback.print_exc()
                continue

    # 3. Relatório Final
    logging.info("="*50)
    logging.info("Resumo da Execução:")
    logging.info(f"Sucessos: {execution_stats['success']}")
    logging.info(f"Erros:    {execution_stats['error']}")
    logging.info(f"Pulados:  {execution_stats['skipped']}")
    logging.info("="*50)

    if execution_stats["error"] > 0:
        logging.warning("O pipeline finalizou com erros.")
        
    else:
        logging.info("Pipeline finalizado com sucesso total.")

if __name__ == "__main__":
    run_pipeline()