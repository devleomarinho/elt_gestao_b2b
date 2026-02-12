{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'produtos_servicos_v2') }}
),

transformed as (
    select
        produto_id,
        nome as nome_produto,
        categoria,        
        
        {{ clean_monetary_values('preco_base') }} as preco_base,        
        {{ clean_percentage('margem_minima') }} as margem_minima_pct,
        {{ clean_percentage('desconto_max') }} as desconto_maximo_pct

    from source
)

select * from transformed