# -*- coding: utf-8 -*-
import os
import json
import logging
import sys
import traceback
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud import storage
import google.auth

# ====================================================================================
# Configurações Globais
# ====================================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações do Projeto
BUCKET_NAME = "datalake-vendas-b2b"  
DATASET_ID = "staging"
LOCATION = "southamerica-east1"

# Mapeamento: Arquivo Local -> Tabela de Destino
# O script assume que os arquivos estão na mesma pasta
FILES_TO_PROCESS = [
    {
        "local_file": "crm_deals_response.json",
        "table_name": "crm_deals",
        "gcs_prefix": "deals"
    },
    {
        "local_file": "crm_contacts_response.json",
        "table_name": "crm_contacts",
        "gcs_prefix": "contacts"
    },
    {
        "local_file": "crm_organizations_response.json",
        "table_name": "crm_organizations",
        "gcs_prefix": "organizations"
    },
    {
        "local_file": "crm_activities_response.json",
        "table_name": "crm_activities",
        "gcs_prefix": "activities"
    }
]

# ====================================================================================
# Autenticação (ADC)
# ====================================================================================
def get_authenticated_clients():
    try:
        creds, project_id = google.auth.default()
        storage_client = storage.Client(credentials=creds, project=project_id)
        bq_client = bigquery.Client(credentials=creds, project=project_id)
        return storage_client, bq_client, project_id
    except Exception as e:
        logging.critical(f"Falha na autenticação: {e}")
        raise

# ====================================================================================
# Funções de Tratamento de Arquivo
# ====================================================================================
def convert_to_ndjson(local_filepath, output_filepath):
    """
    Lê o JSON original (formato API Pipedrive/Hubspot), extrai a chave 'data'
    e salva em formato NDJSON (Newline Delimited JSON) para o BigQuery.
    """
    try:
        with open(local_filepath, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Verifica estrutura padrão { "data": [...] }
        if isinstance(raw_data, dict) and "data" in raw_data:
            records = raw_data["data"]
        elif isinstance(raw_data, list):
            records = raw_data
        else:
            logging.warning(f"! Estrutura inesperada em {local_filepath}. Tentando usar o root como registro.")
            records = [raw_data]

        if not records:
            logging.warning(f"! Nenhum registro encontrado em {local_filepath}.")
            return False

        # Escreve no formato NDJSON
        with open(output_filepath, 'w', encoding='utf-8') as f_out:
            for record in records:
                # Adiciona metadados de ingestão (Opcional, mas recomendado)
                record['_ingested_at'] = datetime.now(timezone.utc).isoformat()
                f_out.write(json.dumps(record) + '\n')
                
        return True
    except Exception as e:
        logging.error(f"!!! Erro ao converter {local_filepath} para NDJSON: {e}")
        return False

# ====================================================================================
# Operações de Nuvem (GCS e BigQuery)
# ====================================================================================
def upload_to_gcs(storage_client, local_file, bucket_name, gcs_path):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file)
        logging.info(f"☁️ Upload GCS: gs://{bucket_name}/{gcs_path}")
        return f"gs://{bucket_name}/{gcs_path}"
    except Exception as e:
        logging.error(f"!!! Erro upload GCS: {e}")
        raise

def load_gcs_to_bigquery(bq_client, gcs_uri, project_id, dataset_id, table_name):
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Full Refresh
        autodetect=True, # Deixar o BQ inferir o schema (ótimo para camada raw)
        ignore_unknown_values=True # Resiliência a campos novos
    )
    
    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, full_table_id, job_config=job_config
        )
        load_job.result()
        
        table = bq_client.get_table(full_table_id)
        logging.info(f" Tabela {table_name} atualizada: {table.num_rows} linhas.")
    except Exception as e:
        logging.error(f"!!! Erro carga BigQuery para {table_name}: {e}")
        # Dica de debug: imprimir erros do job
        if 'load_job' in locals() and load_job.errors:
            logging.error(f"Detalhes do erro BQ: {load_job.errors}")
        raise

def ensure_dataset(client, project_id):
    dataset_ref = f"{project_id}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.info(f"Criando dataset {DATASET_ID}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset)

# ====================================================================================
# Pipeline Principal
# ====================================================================================
def run_pipeline():
    logging.info(" Iniciando Pipeline CRM (JSON -> GCS -> BigQuery)...")
    
    # 1. Setup
    storage_client, bq_client, project_id = get_authenticated_clients()
    ensure_dataset(bq_client, project_id)
    
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    temp_ndjson = "temp_output.ndjson"

    # 2. Processamento em Lote
    for item in FILES_TO_PROCESS:
        local_file = item["local_file"]
        table_name = item["table_name"]
        
        if not os.path.exists(local_file):
            logging.warning(f"Arquivo não encontrado: {local_file}. Pulando.")
            continue
            
        logging.info(f"--- Processando {local_file} ---")
        
        # A. Converte para NDJSON
        success = convert_to_ndjson(local_file, temp_ndjson)
        if not success:
            continue
            
        # B. Define caminho no Data Lake (Particionado por data)
        gcs_path = f"raw/crm/{item['gcs_prefix']}/{today_str}/{table_name}.json"
        
        # C. Upload
        gcs_uri = upload_to_gcs(storage_client, temp_ndjson, BUCKET_NAME, gcs_path)
        
        # D. Carga no BQ
        load_gcs_to_bigquery(bq_client, gcs_uri, project_id, DATASET_ID, table_name)
    
    # Limpeza
    if os.path.exists(temp_ndjson):
        os.remove(temp_ndjson)
    
    logging.info(" Pipeline CRM finalizado.")

if __name__ == "__main__":
    run_pipeline()