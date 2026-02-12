{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_metas_vendedores') }}
),

deduplicated as (
    select 
        *,        
        row_number() over (
            partition by nome_vendedor, mes_referencia 
            order by meta_valor desc, comissao_pct desc
        ) as rn
    from source
)

select
    nome_vendedor,
    mes_referencia,
    meta_valor,
    meta_qtd_deals,
    comissao_pct
from deduplicated
where rn = 1 