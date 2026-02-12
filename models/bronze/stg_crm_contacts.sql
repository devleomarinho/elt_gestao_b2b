{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'crm_contacts') }}
),

transformed as (
    select
        id as contato_id,
        org_id as organizacao_id,
        owner_id as vendedor_responsavel_id,
        
        name as nome_completo,
        email,
        phone as telefone,
        job_title as cargo,

       add_time as data_criacao,
       update_time as data_atualizacao

    from source
)

select * from transformed