{{ config(materialized='table') }}

SELECT
    organizacao_id,
    nome_empresa,
    setor,
    
    CASE
        WHEN faturamento_anual > 10000000 then 'Enterprise'
        WHEN faturamento_anual > 1000000 then 'Mid-Market'
        ELSE 'SMB'
    END AS porte_empresa,
    cidade,
    estado,
    pais
from {{ ref('stg_crm_organizations') }}