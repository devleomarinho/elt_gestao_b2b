{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'crm_organizations') }}
),

transformed as (
    select
        id as organizacao_id,
        name as nome_empresa,
        owner_id as vendedor_responsavel_id,       
        
        address as endereco_completo,
        city as cidade,
        state as estado,
        country as pais,
        postal_code as cep,       
       
        industry as setor,
        employees as faixa_funcionarios,
        safe_cast(annual_revenue as numeric) as faturamento_anual,        
        
        safe_cast(people_count as int64) as qtd_contatos,
        safe_cast(open_deals_count as int64) as qtd_deals_abertos,
        safe_cast(won_deals_count as int64) as qtd_deals_ganhos,

        add_time  as data_criacao,
        _ingested_at  as data_ingestao

    from source
)

select * from transformed