{{ config(
    materialized='table',
    schema='gold'
) }}

with vendas as (
    select * from {{ ref('fct_vendas') }}
),

vendedores as (
    select * from {{ ref('dim_vendedores') }}
),

organizacoes as (
    select * from {{ ref('dim_organizacoes') }}
)

select
    
    v.deal_id,
    v.titulo,
    v.valor_estimado,
    v.valor_ponderado_pipeline,
    v.probabilidade_pct,
    v.etapa_funil,
    v.status_deal,
    v.dias_ciclo_vendas,
    
    vend.nome_vendedor,

    org.nome_empresa,
    org.setor,
    org.porte_empresa,
    org.cidade as cidade_cliente,
    org.estado as estado_cliente,
    
   
    v.data_criacao,
    v.data_previsao_fechamento,
    v.data_ganho,
    
    
    date_trunc(date(v.data_criacao), month) as mes_criacao_deal,
    date_trunc(date(v.data_ganho), month) as mes_fechamento_deal

from vendas v
left join vendedores vend on v.sk_vendedor = vend.sk_vendedor
left join organizacoes org on v.fk_organizacao = org.organizacao_id