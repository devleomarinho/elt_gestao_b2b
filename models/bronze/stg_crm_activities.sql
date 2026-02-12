{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'crm_activities') }}
),

transformed as (
    select
        safe_cast(regexp_extract(id, r'\d+') as int64) as atividade_id,        
        safe_cast(regexp_extract(deal_id, r'\d+') as int64) as deal_id,
        person_id as contato_id,
        org_id as organizacao_id,
        owner_id as vendedor_id,

        type as tipo_atividade, 
        subject as assunto,
        note as observacao,
        done as realizado_bool,
        
       
        safe_cast(due_date as date) as data_vencimento,
        due_time as hora_vencimento,
        safe_cast(duration as string) as duracao_bruta, 

        add_time as data_criacao,
        marked_as_done_time as data_conclusao

    from source
)

select * from transformed