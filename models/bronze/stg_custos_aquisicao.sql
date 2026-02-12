{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'custos_aquisicao_v2') }}
),

transformed as (
    select
        
        {{ clean_dates('data') }} as data_custo,
        
        
        case 
            when lower(origem_lead) like '%gogle%' then 'Google Ads'
            when lower(origem_lead) like '%face%' then 'Facebook Ads'
            when lower(origem_lead) like '%linkedin%' then 'LinkedIn Ads'
            else origem_lead 
        end as canal_aquisicao,
        
        safe_cast(custo as numeric) as custo_total,
        safe_cast(quantidade_leads as int64) as qtd_leads,
        observacoes,
        source_sheet as origem_arquivo

    from source
    where data is not null
)

select * from transformed