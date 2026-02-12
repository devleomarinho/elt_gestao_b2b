{{ config(materialized='table') }}

SELECT 
    
    produto_id,
    nome_produto,
    categoria,
    preco_base,
    margem_minima_pct,
    desconto_maximo_pct

FROM {{ ref('stg_produtos') }}