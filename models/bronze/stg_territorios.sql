{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'territorios_segmentos') }}
),

transformed as (
    select
        territorio as nome_territorio,
        estado as lista_estados_string, 
        segmento_foco,
        vendedor_responsavel as nome_vendedor,
        safe_cast(potencial_anual as numeric) as potencial_anual_mercado

    from source
)

select * from transformed