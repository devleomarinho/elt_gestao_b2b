{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'crm_deals') }}
),

transformed as (
    select
        
        id  as deal_id,
        org_id  as organizacao_id,
        person_id  as contato_id,
        owner_id  as vendedor_id,        
        title as titulo,
        safe_cast(value as numeric) as valor_estimado,
        currency as moeda,
        safe_cast(probability as int64) as probabilidade_pct,
        status as status_deal, 
        stage as etapa_funil,
        stage_id as etapa_id,

        -- Achatando o STRUCT 'custom_fields'
        custom_fields.territorio as territorio_label,
        custom_fields.motivo_perda,
        custom_fields.segmento,
        custom_fields.origem_lead,
        custom_fields.concorrente,
        
        custom_fields.produtos as lista_interesse_produtos,

        safe_cast(expected_close_date as date) as data_previsao_fechamento,
        add_time  as data_criacao,
        update_time  as data_atualizacao,
        won_time  as data_ganho,
        lost_time  as data_perda,
        _ingested_at as data_ingestao

    from source
)

select * from transformed