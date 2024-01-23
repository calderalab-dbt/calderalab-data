-- depends_on: {{ ref('dim_customer_shopify') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_customer_%') %}

select * {{exclude()}} (row_num, order_date) from (
select
{{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_key,
customer_id,
email,
order_date,
last_order_date,
acquisition_date,
acquisition_channel,
last_updated_date as effective_start_date,
cast(null as date) as effective_end_date,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
row_number() over(partition by customer_id order by acquisition_date) row_num
from (
select *,
MAX(order_date) OVER (PARTITION BY customer_id) AS last_order_date,
MIN(order_date) OVER (PARTITION BY customer_id) AS acquisition_date
from (
{% for i in relations %}
    select 
    customer_id,
    email,
    order_date,
    acquisition_channel,
    last_updated_date
    from {{i}}   
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
))) where row_num = 1
