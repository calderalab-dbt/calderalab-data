-- depends_on: {{ ref('dim_transactions_shopify') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_transactions_%') %}

{% for i in relations %}
        select 
        {{ dbt_utils.generate_surrogate_key(['order_id','platform_name']) }} AS order_key,
        platform_name as order_platform,
        transaction_id,
        transaction_stage,
        payment_gateway,
        message,
        payment_mode,
        payment_status,
        last_updated_date as effective_start_date,
        cast(null as date) as effective_end_date,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	    from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}



