{{ config(materialized='table') }}

with deals as (
    select * from {{ ref('stg_crm_deals') }}
),

vendedores as (
    select * from {{ ref('dim_vendedores') }}
),

organizacoes as (
    select * from {{ ref('dim_organizacoes') }}
)

select
    d.deal_id, 
    d.titulo,
    d.valor_estimado,
    d.probabilidade_pct,    
    
    v.sk_vendedor,
    o.organizacao_id as fk_organizacao,
    
    d.etapa_funil,
    d.status_deal,
    
    date_diff(
        coalesce(d.data_ganho, d.data_perda, current_timestamp()), 
        d.data_criacao, 
        DAY
    ) as dias_ciclo_vendas,
    
    (d.valor_estimado * d.probabilidade_pct / 100) as valor_ponderado_pipeline,
    
    d.data_criacao,
    d.data_previsao_fechamento,
    d.data_ganho

from deals d

left join vendedores v on d.vendedor_id = v.vendedor_id
left join organizacoes o on d.organizacao_id = o.organizacao_id