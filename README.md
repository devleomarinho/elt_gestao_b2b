# Pipeline de Engenharia de Dados B2B (ELT Modern Data Stack)

![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

Este repositório contém um projeto *end-to-end* de Engenharia de Dados focado em Inteligência Comercial B2B. O pipeline implementa uma arquitetura **ELT (Extract, Load, Transform)** utilizando a stack moderna da Google Cloud Platform (GCP) e dbt Cloud, projetada para ser escalável e de baixo custo.

---

## Visão Geral do Projeto


Empresas de médio porte frequentemente sofrem com dados fragmentados: vendas no CRM, metas em planilhas e custos de marketing em plataformas isoladas. Este projeto resolve esse problema criando uma fonte centralizada e estruturada de dados, permitindo:
1.  Acompanhamento automático de **Metas vs. Realizado**.
2.  Cálculo de **ROI e CAC** cruzando dados de Marketing e Vendas.
3.  Histórico de evolução do funil de vendas (**SCD Type 2**).
4.  Eliminação de processos manuais, sujeitos a erro humano

### Arquitetura da Solução
O projeto opera no modelo **Serverless Scale-to-Zero**, garantindo custo operacional próximo a zero, funcionando dentro do Free Tier dos serviços da Google Cloud.

<img width="6235" height="2115" alt="diagram" src="https://github.com/user-attachments/assets/ee9917c6-c0e1-4dd2-a839-b0c657ad756e" />

### Estrutura do Repositório

```
/
├── dbt_b2b_project/        # Projeto de Transformação (dbt)
│   ├── models/             # Lógica SQL (Bronze, Silver, Gold)
│   ├── snapshots/          # Historiamento (SCD Type 2)
│   ├── macros/             # Funções reutilizáveis (Jinja)
│   ├── seeds/              # Arquivos estáticos
│   └── dbt_project.yml     # Configurações do dbt
│
├── ingestion/              # Scripts de Extração e Carga
│   ├── main_sheets.py      # Ingestão Google Sheets -> BigQuery
│   ├── main_crm.py         # Ingestão JSON/API -> GCS -> BigQuery
│   ├── Dockerfile          # Definição do container único
│   ├── requirements.txt    # Dependências Python
│   └── data/               # Dados originais
│
└── README.md               # Documentação do projeto
```

### Tech Stack e Destaques Técnicos

**1. Ingestão** 

- Python: Scripts preparados para execução no Google Cloud Run para conexão com APIs e planilhas.

- Docker: Containerização para garantir reprodutibilidade do ambiente.

- Google Cloud Run Jobs: Execução batch serverless (só paga pelos segundos de execução).

- Google Cloud Storage: Data Lake para armazenamento de dados brutos (JSON).

**2. Transformação**

dbt (Data Build Tool): Modelagem de dados modular.

Arquitetura Medalhão:

- Bronze: Limpeza de typos, padronização de tipos e flattening de JSONs.

- Silver: Deduplicação avançada (Window Functions), Star Schema e integridade referencial.

- Gold: Tabelas agregadas (OBT) prontas para BI.

- Data Quality: Tratamento de erros propositais na fonte (ex: metas duplicadas, formatação de moeda incorreta "R$", acentuação inconsistente).

- Snapshots: Rastreamento de mudanças históricas no funil de vendas.

**Como Executar o Projeto**

Pré-requisitos:

- Conta na Google Cloud Platform (GCP).

- Conta no dbt Cloud (Plano Free).

- Google Cloud CLI instalada.

#### Passo 1: Ingestão ####

Após clonar o repositório, navegue até a pasta ingestion/ e faça o build da imagem Docker:

```
cd ingestion
gcloud builds submit --tag gcr.io/SEU_PROJECT_ID/ingestao-b2b:v1
```

Crie e execute os Jobs no Cloud Run:

```
# Job para Sheets
gcloud run jobs create job-sheets --image gcr.io/SEU_PROJECT_ID/ingestao-b2b:v1 --command python --args main_sheets.py
gcloud run jobs execute job-sheets

# Job para CRM
gcloud run jobs create job-crm --image gcr.io/SEU_PROJECT_ID/ingestao-b2b:v1 --command python --args main_crm.py
gcloud run jobs execute job-crm
```

#### Passo 2: Transformação (dbt) ####

No dbt Cloud (ou Core), configure a conexão com o BigQuery e execute:

```
dbt snapshot  # Cria a tabela de histórico (primeira carga)
dbt build     # Executa seeds, models e testes
```

### Dicionário de Dados - Resumo
O Data Mart final (Camada Gold) entrega as seguintes tabelas principais:

| Tabela | Descrição |
| :--- | :--- |
| `obt_vendas_analitico` | Tabela única com detalhes da venda, cliente e vendedor. |
| `agg_metas_vs_realizado` | Acompanhamento mensal de atingimento de metas por vendedor. |
| `agg_performance_marketing` | ROI, CAC e CPL por canal de aquisição (cruzamento Marketing x Vendas). |

### Estimativa de Custos (FinOps)
Este projeto foi desenhado para rodar dentro do Free Tier da GCP para volumes de dados de PMEs:

- Cloud Run: Grátis (dentro dos 2 milhões de requisições/mês).

- BigQuery: Grátis (10GB armazenamento + 1TB query/mês).

### Contato

- Projeto desenvolvido por Leonardo Marinho.
[LinkedIn](https://www.linkedin.com/in/devleomarinho/) | [Email](dev.leomarinho@gmail.com)
