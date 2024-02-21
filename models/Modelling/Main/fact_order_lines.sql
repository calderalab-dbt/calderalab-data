
-- depends_on: {{ ref('fact_order_lines_amazonseller') }}
-- depends_on: {{ ref('fact_order_lines_shopify') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'fact_order_lines_%') %}

{% for i in relations %}
    select 
    {{ dbt_utils.generate_surrogate_key(['order_id','platform_name']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['platform_name','store_name']) }} AS platform_key,
    {{ dbt_utils.generate_surrogate_key(['brand']) }} AS brand_key,
    {{ dbt_utils.generate_surrogate_key(['product_id', 'sku','platform_name']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['subscription_id','sku']) }} AS subscription_key,
    date,
    transaction_type,
    reason,
    quantity,
    case when quantity != 0 then round((subtotal_price/exchange_currency_rate),2)/quantity else null end as unit_price,
    round((total_price/exchange_currency_rate),2) as item_total_price,
    round((subtotal_price/exchange_currency_rate),2) as item_subtotal_price,
    round((total_tax/exchange_currency_rate),2) as item_total_tax,
    round((shipping_price/exchange_currency_rate),2) as item_shipping_price,
    round((giftwrap_price/exchange_currency_rate),2) as item_giftwrap_price,
    round((item_discount/exchange_currency_rate),2) as item_discount,
    round((shipping_discount/exchange_currency_rate),2) as item_shipping_discount,
    exchange_currency_code as currency_code,
    is_cancelled,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}