
{% if var('ShopifyOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%caldera_us_shopify_orders%')}} and lower(table_name) not like '%shopify%fulfillment_orders' and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
{% endset %}  

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
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


    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(a.id as string) as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
        currency,
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        {% if target.type =='snowflake' %}
        discount_codes.VALUE:code::VARCHAR as discount_code,
        discount_codes.VALUE:amount::NUMERIC as discount_amount,
        discount_codes.VALUE:type::VARCHAR as discount_type,
        {% else %}
        discount_codes.code as discount_code,
        discount_codes.amount as discount_amount,
        discount_codes.type as discount_type,
        {% endif %}
        email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        name,
        note_attributes,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        phone,
        presentment_currency,
        CAST({{ timezone_conversion("a.processed_at") }} AS TIMESTAMP) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        subtotal_price,
        subtotal_price_set,
        tags,
        tax_lines,
        taxes_included,
        test,
        token,
        total_discounts,
        total_discounts_set,
        total_line_items_price,
        total_line_items_price_set,
        total_outstanding,
        total_price,
        total_price_set,
        total_price_usd,
        total_shipping_price_set,
        total_tax,
        total_tax_set,
        total_tip_received,
        total_weight,
        CAST({{ timezone_conversion("a.updated_at") }} AS TIMESTAMP) as updated_at,
        billing_address,
        customer,
        discount_applications,
        fulfillments,
        line_items,
        payment_details,
        refunds,
        shipping_address,
        shipping_lines,
        app_id,
        customer_locale,
        note,
        closed_at,
        fulfillment_status,
        location_id,
        cancel_reason,
        cancelled_at,
        user_id,
        -- device_id,
        {% if var('currency_conversion_flag') %}
            case when b.value is null then 1 else b.value end as exchange_currency_rate,
            case when b.from_currency_code is null then currency else b.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        Dense_Rank() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
                {% endif %}
                {{unnesting("discount_codes")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
