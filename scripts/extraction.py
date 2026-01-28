# -*- coding: utf-8 -*-
import os
import json
import gspread
import pandas as pd
import traceback
from datetime import datetime, UTC
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account
import re
import unicodedata

# ====================================================================================
# Autenticação
# ====================================================================================
google_gcp_json_str = os.environ.get('elt_googleGCP') # chave de serviço 
if not google_gcp_json_str:
    raise ValueError("A variável de ambiente 'googleGCP' não foi definida no Render!")

info_credenciais = json.loads(google_gcp_json_str)
if isinstance(info_credenciais, str):
    print("(!) JSON duplamente encodado detectado. Decodificando novamente...")
    info_credenciais = json.loads(info_credenciais)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
]
credentials = service_account.Credentials.from_service_account_info(
    info_credenciais, scopes=SCOPES
)

PROJECT_ID = info_credenciais['project_id']
gc = gspread.authorize(credentials)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
print("✅ Clientes Google Sheets e BigQuery autorizados com sucesso!")
# ====================================================================================

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

# ====================================================================================
# Utilidades
# ====================================================================================
def sanitize_tablename(tab_name):
    name = unicodedata.normalize("NFKD", str(tab_name)).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[\W]+", "_", name)
    return name.strip("_").lower()


def safe_get_records(worksheet):
    """
    Lê os valores *formatados* exatamente como aparecem no Sheets (sem numericizar).
    """
    try:
        values = worksheet.get_all_values(value_render_option='FORMATTED_VALUE')
        if not values:
            return pd.DataFrame()
        header, rows = values[0], values[1:]
        df = pd.DataFrame(rows, columns=header)
        df = df.replace({"": None})
        return df
    except gspread.exceptions.APIError as e:
        print(f" Erro de API ao ler a planilha: {e}")
        return pd.DataFrame()
    except Exception:
        print(" Ocorreu um erro inesperado ao ler a planilha:")
        traceback.print_exc()
        return pd.DataFrame()

def sanitize_bq_column_name(column_name):
    name = unicodedata.normalize('NFKD', str(column_name)).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^\w_]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if not name or name[0].isdigit():
        name = 'col_' + name
    return name[:300].lower() or 'column'

def sanitize_dataframe_columns(df):
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
    Converte todas as colunas para string, preservando None onde houver vazios/NaN.
    """
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(lambda x: None if pd.isna(x) else str(x))
    return out

def to_snake_case(text: str) -> str:
    """
    Converte string para snake_case: minúsculas, espaços e caracteres especiais transformados em underscore.
    """
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[\W]+', '_', text)
    text = re.sub(r'_+', '_', text)
    text = text.strip('_')
    return text
# ====================================================================================
# Preparação + Schema (tudo STRING)
# ====================================================================================
def prepare_data_and_generate_schema_all_string(df):
    df_prepared = df_all_to_string(df.copy())
    bq_schema = [{"name": col, "type": "STRING"} for col in df_prepared.columns]
    return df_prepared, bq_schema

# ====================================================================================
# Upload para BigQuery (STRING)
# ====================================================================================
def upload_to_bq_landing(client: bigquery.Client, df: pd.DataFrame,
                         table_name: str, project_id: str, dataset_id: str):
    try:
        df_sanitized = sanitize_dataframe_columns(df.copy())

        # Tudo vira string
        df_prepared, bq_schema = prepare_data_and_generate_schema_all_string(df_sanitized)

        full_table_id = f"{project_id}.{dataset_id}.{table_name}"
        print(f"=-=-=- Enviando {len(df_prepared)} registros para {full_table_id}")

        schema_fields = [bigquery.SchemaField(f["name"], f["type"]) for f in bq_schema]
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema_fields
        )

        load_job = client.load_table_from_dataframe(
            df_prepared, full_table_id, job_config=job_config, location='southamerica-east1'
        )
        load_job.result()

        table = client.get_table(full_table_id)
        print(f"**************** Sucesso: {table.num_rows} registros totais em '{table_name}' ***************")
        return True
    except Exception:
        print(f" !!! Erro CRÍTICO no upload para a tabela {table_name}. Detalhes:")
        traceback.print_exc()
        raise

# ====================================================================================
# Helpers do pipeline
# ====================================================================================
def ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str,
                   location: str = 'southamerica-east1'):
    try:
        client.get_dataset(f"{project_id}.{dataset_id}")
        print(f"✅ Dataset '{dataset_id}' já existe.")
    except Exception:
        print(f"⚠️ Dataset '{dataset_id}' não encontrado. Criando...")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = location
        client.create_dataset(dataset, timeout=30)
        print(f"✅ Dataset '{dataset_id}' criado.")

# ====================================================================================
# Pipeline
# ====================================================================================
def landing_pipeline(gspread_client, bigquery_client, project_id, dataset_id):
    print("Iniciando pipeline de landing...")
    ensure_dataset(bigquery_client, project_id, dataset_id)
    execution_timestamp = datetime.now(UTC)
    batch_id = execution_timestamp.strftime("%Y%m%d%H%M%S%f-3")
    print(f"Batch ID: {batch_id}")

    for config in SHEETS_CONFIG:
        sheet_name = config["sheet_name"]
        sheet_id = config["sheet_id"]
        for tab in config["tabs"]:
            try:
                sheet = gspread_client.open_by_key(sheet_id)
                try:
                    worksheet = sheet.worksheet(tab)
                except Exception:
                    print(f"!!! Aba '{tab}' não encontrada na planilha {sheet_name}. Pulando...")
                    continue
                df = safe_get_records(worksheet)
                df = df.dropna(how='all')
                if df.empty:
                    print(f"!!! Aba '{tab}' vazia em {sheet_name} - pulando...")
                    continue
                raw_source = f"{tab}"
                df['source_sheet'] = to_snake_case(raw_source)
                table_name = sanitize_tablename(tab)
                print(f"Coletados {len(df)} registros da aba '{tab}' da planilha '{sheet_name}'. Nome tabela: '{table_name}'")
                upload_to_bq_landing(bigquery_client, df.copy(), table_name, project_id, dataset_id)
            except Exception:
                print(f"!!! Erro ao processar planilha '{sheet_name}', aba '{tab}'. Detalhes:")
                traceback.print_exc()
                continue
    print("Pipeline concluído com sucesso!")
    return True

# ====================================================================================
# Execução
# ====================================================================================
if __name__ == "__main__":
    DATASET_ID = "staging"
    landing_pipeline(
        gspread_client=gc,
        bigquery_client=bq_client,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )








