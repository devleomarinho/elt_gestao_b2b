*Read this in other languages: [English](README.md), [Português](README.pt-br.md).*

---

# B2B Data Engineering Pipeline (ELT Modern Data Stack)

![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

This repository contains an *end-to-end* Data Engineering project focused on B2B Commercial Intelligence. The pipeline implements an **ELT** architecture using the modern data stack on Google Cloud Platform (GCP) and dbt Cloud, designed to be scalable and cost-effective.

---

## Project Overview

Mid-sized companies often suffer from fragmented data: sales in a CRM, goals in spreadsheets, and marketing costs in isolated platforms. This project solves this problem by creating a centralized and structured data source, enabling:
1.  Automatic tracking of **Goals vs. Actuals**.
2.  Calculation of **ROI and CAC** by joining Marketing and Sales data.
3.  Historical tracking of the sales funnel evolution (**SCD Type 2**).
4.  Elimination of manual processes, which are prone to human error.

### Solution Architecture
The project operates on a **Serverless Scale-to-Zero** model, ensuring an operational cost close to zero by running entirely within the Google Cloud Free Tier.

<img width="6235" height="2115" alt="diagram" src="https://github.com/user-attachments/assets/ee9917c6-c0e1-4dd2-a839-b0c657ad756e" />

### Repository Structure

```
/
├── dbt_b2b_project/        # Transformation Project (dbt)
│   ├── models/             # SQL Logic (Bronze, Silver, Gold)
│   ├── snapshots/          # History tracking (SCD Type 2)
│   ├── macros/             # Reusable functions (Jinja)
│   ├── seeds/              # Static files
│   └── dbt_project.yml     # dbt configurations
│
├── ingestion/              # Extraction and Loading Scripts
│   ├── main_sheets.py      # Google Sheets -> BigQuery Ingestion
│   ├── main_crm.py         # JSON/API -> GCS -> BigQuery Ingestion
│   ├── Dockerfile          # Unified container definition
│   ├── requirements.txt    # Python dependencies
│   └── data/               # Original data
│
└── README.md               # Project documentation
```

### Tech Stack and Technical Highlights

**1. Ingestion** - Python: Scripts prepared to run on Google Cloud Run to connect with APIs and spreadsheets.

- Docker: Containerization to ensure environment reproducibility.

- Google Cloud Run Jobs: Serverless batch execution (pay only for execution seconds).

- Google Cloud Storage: Data Lake for storing raw data (JSON).

**2. Transformation**

dbt (Data Build Tool): Modular data modeling.

Medallion Architecture:

- Bronze: Typo cleaning, type standardization, and JSON flattening.

- Silver: Advanced deduplication (Window Functions), Star Schema, and referential integrity.

- Gold: Aggregated tables (OBT) ready for BI.

- Data Quality: Handling intentional errors at the source (e.g., duplicate goals, incorrect currency formatting "R$", inconsistent accents).

- Snapshots: Historical tracking of sales funnel changes.

**How to Run the Project**

Prerequisites:

- Google Cloud Platform (GCP) account.

- dbt Cloud account (Free Tier).

- Google Cloud CLI installed.

#### Step 1: Ingestion ####

After cloning the repository, navigate to the `ingestion/` folder and build the Docker image:

```
cd ingestion
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/ingestao-b2b:v1
```

Create and execute the Jobs in Cloud Run:

**Job for Google Sheets**

```
gcloud run jobs create job-sheets --image gcr.io/YOUR_PROJECT_ID/ingestao-b2b:v1 --command python --args main_sheets.py
gcloud run jobs execute job-sheets
```

**Job for CRM**

```
gcloud run jobs create job-crm --image gcr.io/YOUR_PROJECT_ID/ingestao-b2b:v1 --command python --args main_crm.py
gcloud run jobs execute job-crm
```

#### Step 2: Transformation (dbt) ####

In dbt Cloud (or Core), configure the connection to BigQuery and execute:

```
dbt snapshot  # Creates the history table (first load)
dbt build     # Executes seeds, models, and tests
```

### Data Dictionary - Summary
The final Data Mart (Gold Layer) delivers the following main tables:

| Table | Description |
| :--- | :--- |
| `obt_vendas_analitico` | Single table with detailed information on sales, clients, and sales reps. |
| `agg_metas_vs_realizado` | Monthly tracking of goal achievement by sales rep. |
| `agg_performance_marketing` | ROI, CAC, and CPL per acquisition channel (Marketing x Sales join). |

### Cost Estimation (FinOps)
This project was designed to run within the GCP Free Tier for SME data volumes:

- Cloud Run: Free (within the 2 million requests/month limit).

- BigQuery: Free (10GB storage + 1TB query/month).

### Contact

- Project developed by Leonardo Marinho.
[LinkedIn](https://www.linkedin.com/in/devleomarinho/) | [Email](mailto:dev.leomarinho@gmail.com)
