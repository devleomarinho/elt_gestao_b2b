{{ config(
    materialized='table',
    schema='gold'
) }}

with vendas_por_mes as (
    
    select
        v.sk_vendedor,
        date_trunc(date(v.data_ganho), month) as mes_referencia,
        sum(v.valor_estimado) as total_vendido,
        count(v.deal_id) as qtd_deals_fechados
    from {{ ref('fct_vendas') }} v
    where v.status_deal = 'won' 
    group by 1, 2
),

metas as (
    
    select
        
        d.sk_vendedor,
        m.mes_referencia,
        m.meta_valor,
        m.meta_qtd_deals
    from {{ ref('fct_metas_vendas') }} m
    left join {{ ref('dim_vendedores') }} d on m.nome_vendedor = d.nome_vendedor
),

final as (
    
    select
        coalesce(m.mes_referencia, v.mes_referencia) as mes_referencia,
        coalesce(m.sk_vendedor, v.sk_vendedor) as sk_vendedor,        
        
        coalesce(m.meta_valor, 0) as meta_faturamento,
        coalesce(m.meta_qtd_deals, 0) as meta_qtd,        
        
        coalesce(v.total_vendido, 0) as realizado_faturamento,
        coalesce(v.qtd_deals_fechados, 0) as realizado_qtd,
        
        
        case 
            when m.meta_valor > 0 then (coalesce(v.total_vendido, 0) / m.meta_valor)
            else 0 
        end as atingimento_meta_pct

    from metas m
    full outer join vendas_por_mes v 
        on m.sk_vendedor = v.sk_vendedor 
        and m.mes_referencia = v.mes_referencia
)


select 
    f.*,
    d.nome_vendedor
from final f
left join {{ ref('dim_vendedores') }} d 
on f.sk_vendedor = d.sk_vendedor
where f.sk_vendedor is not null 