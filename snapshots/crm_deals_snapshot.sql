{% snapshot crm_deals_snapshot %}

{{
    config(
      target_database='elt-b2b',      
      target_schema='snapshots',      
      unique_key='id',             
      strategy='check',               
      check_cols=['stage', 'status', 'value', 'expected_close_date', 'probability'] 
    )
}}

select * from {{ source('staging', 'crm_deals') }}

{% endsnapshot %}