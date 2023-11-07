select * {{exclude()}} (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to), 
cast(dbt_valid_from as date) as effective_start_date, 
case when dbt_valid_to is null then '9999-12-31' else cast(dbt_valid_to as date) end as effective_end_date,
dbt_updated_at as last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as run_id,
case when dbt_valid_to is null and end_date is null then 'Active' else 'Non-Active' end as status  
from {{ ref('dim_product_snapshot') }}
