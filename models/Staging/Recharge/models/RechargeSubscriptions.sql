{% if var('RechargeSubscriptions') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
        {{set_table_name('%recharge%subscriptions')}}    
    {% endset %} 

    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

        SELECT * {{exclude()}} (row_num)
        FROM (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            coalesce(CAST(id as string),'') as subscription_id,
            address_id,
            customer_id,
           	CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
            utm_params.utm_source,
            utm_params.utm_medium,
            charge_interval_frequency,
            external_product_id.ecommerce as external_product_id,
            external_variant_id.ecommerce as external_variant_id,
            has_queued_charges,
            is_prepaid,
            is_skippable,
            is_swappable,
            max_retries_reached,
            next_charge_scheduled_at,
            order_interval_frequency,
            order_interval_unit,
            price,
            product_title,
            properties,
            quantity,
            sku,
            sku_override,
            status,
           
            CAST({{ timezone_conversion("a.updated_at") }} AS TIMESTAMP) as updated_at,
            variant_title,
            cancellation_reason,
    
            CAST({{ timezone_conversion("a.cancelled_at") }} AS TIMESTAMP) as cancelled_at,
            order_day_of_month,
            presentment_currency,
            cancellation_reason_comments,
	        a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY a.id,external_product_id.ecommerce,external_variant_id.ecommerce,sku order by a.{{daton_batch_runtime()}} desc, next_charge_scheduled_at desc) row_num
            from {{i}} a
                {{unnesting("ANALYTICS_DATA")}}
                {{multi_unnesting("ANALYTICS_DATA","UTM_PARAMS")}}
                {{unnesting("EXTERNAL_PRODUCT_ID")}}
                {{unnesting("EXTERNAL_VARIANT_ID")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            )
            where row_num =1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
