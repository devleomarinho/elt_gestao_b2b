{{ config(materialized='table') }}

with vendedores_crm as (
    
    select distinct
        safe_cast(owner_id as int64) as vendedor_id,
        owner_name as nome_vendedor,
        'CRM' as origem
    from {{ source('staging', 'crm_deals') }}
    where owner_id is not null
),

vendedores_metas as (
    
    select distinct
        null as vendedor_id, 
        nome_vendedor,
        'Planilha' as origem
    from {{ ref('stg_metas_vendedores') }}
)

select 
    coalesce(vendedor_id, row_number() over (order by nome_vendedor) + 100000) as sk_vendedor,
    * 
from vendedores_crm
