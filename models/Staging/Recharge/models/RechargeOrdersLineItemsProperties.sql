
{% if var('RechargeOrderLineItemsProperties') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
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
        {{set_table_name('%recharge%orders')}}    
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
            CAST(a.id as string) as order_id,
            address_id,
            billing_address,
            charge,
            client_details,
            CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
            currency,
            customer,
            external_order_id.ecommerce as external_order_id,
            external_order_name.ecommerce as external_order_name,
            external_order_number.ecommerce as external_order_number,
            is_prepaid,
            line_items.purchase_item_id,
            line_items.external_inventory_policy,
            line_items.external_product_id,
            line_items.external_variant_id,
            line_items.grams,
            line_items.images,
            properties.name,
            properties.value,
            line_items.purchase_item_type,
            line_items.sku,
            line_items.taxable as line_items_taxable,
            line_items.title as line_items_title,
            line_items.unit_price_includes_tax,
            line_items.variant_title,
            note,
            processed_at,
            scheduled_at,
            shipping_address,
            shipping_lines,
            status,
            tags,
            a.taxable,
            type,
            updated_at,     
            external_cart_token,
            order_attributes,
            -- error,
            a.tax_lines,
            total_duties,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                cast(null as string) as exchange_currency_code, 
            {% endif %}
            a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY id, sku order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {{unnesting("EXTERNAL_ORDER_ID")}}
                {{unnesting("EXTERNAL_ORDER_NAME")}}
                {{unnesting("EXTERNAL_ORDER_NUMBER")}}
                {{unnesting("LINE_ITEMS")}}
                {{multi_unnesting("LINE_ITEMS","PROPERTIES")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            ) where row_num = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
