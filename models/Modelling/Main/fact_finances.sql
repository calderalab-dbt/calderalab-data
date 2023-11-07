-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'fact_finances_%') %}

{% for i in relations %}
        Select 
        {{ dbt_utils.surrogate_key(['order_id','platform_name']) }} AS order_key,
        {{ dbt_utils.surrogate_key(['platform_name','store_name']) }} AS platform_key,
        {{ dbt_utils.surrogate_key(['brand']) }} AS brand_key,
        {{ dbt_utils.surrogate_key(['product_id', 'sku', 'platform_name']) }} AS product_key,
        date,
        amount_type,
        transaction_type,
        charge_type,
        amount,
        exchange_currency_code as currency_code,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	      from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
