{{ config(
  post_hook = "update {{ this }} ord
set ship_address_key=keys.ship_address_key, bill_address_key=keys.bill_address_key
from {{ref('order_address_keys')}}  keys
  where ord.order_key=keys.order_key"
) }}

-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_orders_%') %}

{% for i in relations %}
        select
        {{ dbt_utils.surrogate_key(['order_id','platform_name']) }} AS order_key,
        cast(null as string) as ship_address_key,
        cast(null as string) as bill_address_key,
        platform_name,
        order_id,
        payment_mode,
        order_channel,
        last_updated_date as effective_start_date,
        cast(null as date) as effective_end_date,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
        {% endif %} 
    {% if not loop.last %} union all {% endif %}
{% endfor %}