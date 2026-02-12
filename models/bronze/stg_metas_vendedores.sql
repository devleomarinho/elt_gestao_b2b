{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'metas_vendedores_v2') }}
),

transformed as (
    select
        
        INITCAP({{normalize_names('vendedor')}}) as nome_vendedor,        
        
        {{ clean_dates('mes') }} as mes_referencia,       
       
        safe_cast(meta_receita as numeric) as meta_valor,
        safe_cast(meta_deals_fechados as int64) as meta_qtd_deals,       
        safe_cast(
            substr(regexp_replace(comissao_percentual, r'[^\d]', ''), 1, 4) 
        as numeric) / 1000 as comissao_pct,

    from source
)

select * from transformed
where nome_vendedor is not null