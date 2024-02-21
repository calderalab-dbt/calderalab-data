
-- depends_on: {{ ref('fact_orders_amazonseller') }}
-- depends_on: {{ ref('fact_orders_shopify') }}

-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'fact_orders_%') %}

{% for i in relations %}
        select 
        {{ dbt_utils.generate_surrogate_key(['order_id','platform_name']) }} AS order_key,
        {{ dbt_utils.generate_surrogate_key(['platform_name','store_name']) }} AS platform_key,
        {{ dbt_utils.generate_surrogate_key(['brand']) }} AS brand_key,
        date,
        transaction_type,
        quantity,
        round((total_price/exchange_currency_rate),2) as total_price,
        round((subtotal_price/exchange_currency_rate),2) as subtotal_price,
        round((total_tax/exchange_currency_rate),2) as total_tax,
        round((shipping_price/exchange_currency_rate),2) as shipping_price,
        round((giftwrap_price/exchange_currency_rate),2) as giftwrap_price,
        round((order_discount/exchange_currency_rate),2) as order_discount,
        round((shipping_discount/exchange_currency_rate),2) as shipping_discount,
        exchange_currency_code as currency_code,
        is_cancelled,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}