{% if var('ShopifyOrdersCustomer') %}
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

    select * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'') as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
        a.currency,
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        discount_codes,
        coalesce(a.email,'') as email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        a.name,
        note_attributes,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        a.phone,
        presentment_currency,
        CAST({{ timezone_conversion("a.processed_at") }} AS TIMESTAMP) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        subtotal_price,
        subtotal_price_set,
        a.tags,
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
        {% if target.type =='snowflake' %}
        CUSTOMER.VALUE:id::VARCHAR as customer_id,
        CUSTOMER.VALUE:email::VARCHAR as customer_email,
        CUSTOMER.VALUE:accepts_marketing as customer_accepts_marketing,
        CUSTOMER.VALUE:created_at::TIMESTAMP as customer_created_at,
        CUSTOMER.VALUE:updated_at::TIMESTAMP as customer_updated_at,
        CUSTOMER.VALUE:first_name::VARCHAR as customer_first_name,
        CUSTOMER.VALUE:last_name::VARCHAR as customer_last_name,
        CUSTOMER.VALUE:orders_count as customer_orders_count,
        CUSTOMER.VALUE:state as customer_state,
        CUSTOMER.VALUE:total_spent as customer_total_spent,
        CUSTOMER.VALUE:last_order_id as customer_last_order_id,
        CUSTOMER.VALUE:verified_email as customer_verified_email,
        CUSTOMER.VALUE:tax_exempt as customer_tax_exempt,
        CUSTOMER.VALUE:phone::VARCHAR as customer_phone,
        CUSTOMER.VALUE:tags::VARCHAR as customer_tags,
        CUSTOMER.VALUE:currency::VARCHAR as customer_currency,
        CUSTOMER.VALUE:last_order_name as customer_last_order_name,
        CUSTOMER.VALUE:accepts_marketing_updated_at as customer_accepts_marketing_updated_at,
        CUSTOMER.VALUE:admin_graphql_api_id as customer_admin_graphql_api_id,
        DEFAULT_ADDRESS.VALUE:id::VARCHAR as default_address_id,
        DEFAULT_ADDRESS.VALUE:customer_id::VARCHAR as default_address_customer_id,
        DEFAULT_ADDRESS.VALUE:address1::VARCHAR as default_address_address1,
        DEFAULT_ADDRESS.VALUE:address2::VARCHAR as default_address_address2,
        DEFAULT_ADDRESS.VALUE:city::VARCHAR as default_address_city,
        DEFAULT_ADDRESS.VALUE:province::VARCHAR as default_address_province,
        DEFAULT_ADDRESS.VALUE:country::VARCHAR as default_address_country,
        DEFAULT_ADDRESS.VALUE:zip::VARCHAR as default_address_zip,
        DEFAULT_ADDRESS.VALUE:phone::VARCHAR as default_address_phone,
        DEFAULT_ADDRESS.VALUE:name::VARCHAR as default_address_name,
        DEFAULT_ADDRESS.VALUE:province_code as default_address_province_code,
        DEFAULT_ADDRESS.VALUE:country_code as default_address_country_code,
        DEFAULT_ADDRESS.VALUE:country_name as default_address_country_name,
        DEFAULT_ADDRESS.VALUE:default as default_address_default,
        DEFAULT_ADDRESS.VALUE:first_name::VARCHAR as default_address_first_name,
        DEFAULT_ADDRESS.VALUE:last_name::VARCHAR as default_address_last_name,
        DEFAULT_ADDRESS.VALUE:company::VARCHAR as default_address_company,
        {% else %}
        cast(CUSTOMER.id as string) as customer_id,
        CUSTOMER.email as customer_email,
        CUSTOMER.accepts_marketing as customer_accepts_marketing,
        CUSTOMER.created_at as customer_created_at,
        CUSTOMER.updated_at as customer_updated_at,
        CUSTOMER.first_name as customer_first_name,
        CUSTOMER.last_name as customer_last_name,
        CUSTOMER.orders_count as customer_orders_count,
        CUSTOMER.state as customer_state,
        CUSTOMER.total_spent as customer_total_spent,
        CUSTOMER.last_order_id as customer_last_order_id,
        CUSTOMER.verified_email as customer_verified_email,
        CUSTOMER.tax_exempt as customer_tax_exempt,
        CUSTOMER.phone as customer_phone,
        CUSTOMER.tags as customer_tags,
        CUSTOMER.currency as customer_currency,
        CUSTOMER.last_order_name as customer_last_order_name,
        CUSTOMER.accepts_marketing_updated_at as customer_accepts_marketing_updated_at,
        CUSTOMER.admin_graphql_api_id as customer_admin_graphql_api_id,
        DEFAULT_ADDRESS.id as default_address_id,
        DEFAULT_ADDRESS.customer_id as default_address_customer_id,
        DEFAULT_ADDRESS.address1 as default_address_address1,
        DEFAULT_ADDRESS.address2 as default_address_address2,
        DEFAULT_ADDRESS.city as default_address_city,
        DEFAULT_ADDRESS.province as default_address_province,
        DEFAULT_ADDRESS.country as default_address_country,
        DEFAULT_ADDRESS.zip as default_address_zip,
        DEFAULT_ADDRESS.phone as default_address_phone,
        DEFAULT_ADDRESS.name as default_address_name,
        DEFAULT_ADDRESS.province_code as default_address_province_code,
        DEFAULT_ADDRESS.country_code as default_address_country_code,
        DEFAULT_ADDRESS.country_name as default_address_country_name,
        DEFAULT_ADDRESS.default as default_address_default,
        DEFAULT_ADDRESS.first_name as default_address_first_name,
        DEFAULT_ADDRESS.last_name as default_address_last_name,
        DEFAULT_ADDRESS.company as default_address_company,
        {% endif %}
        discount_applications,
        fulfillments,
        line_items,
        payment_details,
        refunds,
        shipping_address,
        shipping_lines,
        app_id,
        customer_locale,
        a.note,
        closed_at,
        fulfillment_status,
        location_id,
        cancel_reason,
        cancelled_at,
        user_id,
        -- device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        DENSE_RANK() OVER (PARTITION BY a.id, CUSTOMER.VALUE:id order by a._daton_batch_runtime desc) row_num
        {% else %}
        DENSE_RANK() OVER (PARTITION BY a.id, CUSTOMER.id order by a._daton_batch_runtime desc) row_num
        {% endif %}
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("CUSTOMER")}}
            {{multi_unnesting("CUSTOMER","DEFAULT_ADDRESS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    )
    where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}


