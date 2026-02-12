{{ config(
    materialized='table',
    schema='gold'
) }}

with custos as (
    select
        date_trunc(data_custo, month) as mes,
        canal_aquisicao,
        sum(custo_total) as investimento_total,
        sum(qtd_leads) as leads_totais
    from {{ ref('stg_custos_aquisicao') }}
    group by 1, 2
),

performance_vendas as (
    select 
        
        d.origem_lead as canal_aquisicao,
        date_trunc(date(d.data_criacao), month) as mes,
        count(d.deal_id) as oportunidades_geradas,
        count(case when d.status_deal = 'won' then 1 end) as vendas_realizadas,
        sum(case when d.status_deal = 'won' then d.valor_estimado else 0 end) as receita_gerada
    from {{ ref('stg_crm_deals') }} d
    group by 1, 2
)

select
    coalesce(c.mes, p.mes) as mes_referencia,
    coalesce(c.canal_aquisicao, p.canal_aquisicao) as canal,
    
   
    coalesce(c.investimento_total, 0) as custo_marketing,
    coalesce(c.leads_totais, 0) as leads_captados,
    
    
    coalesce(p.oportunidades_geradas, 0) as deals_criados,
    coalesce(p.vendas_realizadas, 0) as deals_ganhos,
    coalesce(p.receita_gerada, 0) as receita_total,
    
    
    case when coalesce(c.leads_totais, 0) > 0 
         then c.investimento_total / c.leads_totais 
         else 0 end as cpl,
         
    
    case when coalesce(p.vendas_realizadas, 0) > 0 
         then c.investimento_total / p.vendas_realizadas 
         else 0 end as cac,
         
    
    case when coalesce(c.investimento_total, 0) > 0 
         then (p.receita_gerada - c.investimento_total) / c.investimento_total 
         else 0 end as roi

from custos c
full outer join performance_vendas p 
    on c.mes = p.mes 
    and lower(c.canal_aquisicao) = lower(p.canal_aquisicao)