{% snapshot dim_product_snapshot %}

{{
    config(
      target_schema='edm_modelling_snapshot',
      unique_key='product_key',
      strategy='check',
      check_cols= ['start_date','end_date']
    )
}}

select * from {{ ref('dim_product') }}

{% endsnapshot %}