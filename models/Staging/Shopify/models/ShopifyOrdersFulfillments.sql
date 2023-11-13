{% if var('ShopifyOrdersFulfillments') %}
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
        cast(a.id as string) as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        cast(a.created_at as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        discount_codes,
        email,
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
        phone,
        presentment_currency,
        CAST(processed_at as timestamp) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        subtotal_price,
        subtotal_price_set,
        tags,
        a.tax_lines,
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
        -- CAST(a.updated_at as {{ dbt.type_timestamp() }}) as updated_at,
        {{ timezone_conversion("a.updated_at") }} as updated_at,
        billing_address,
        customer,
        discount_applications,
        {% if target.type =='snowflake' %}
        COALESCE(fulfillments.VALUE:id::VARCHAR,'') as fulfillments_id,
        fulfillments.VALUE:admin_graphql_api_id as fulfillments_admin_graphql_api_id,
        fulfillments.VALUE:created_at as fulfillments_created_at,
        fulfillments.VALUE:location_id as fulfillments_location_id,
        fulfillments.VALUE:name as fulfillments_name,
        fulfillments.VALUE:order_id::VARCHAR as fulfillments_orders_id,
        receipt.VALUE:testcase as receipt_testcase,
        receipt.VALUE:authorization as receipt_authorization,
        receipt.VALUE:gift_cards as receipt_gift_cards,
        fulfillments.VALUE:service as fulfillments_service,
        fulfillments.VALUE:status as fulfillments_status,
        fulfillments.VALUE:tracking_company as fulfillments_tracking_company,
        fulfillments.VALUE:tracking_number as fulfillments_tracking_number,
        fulfillments.VALUE:tracking_numbers as fulfillments_tracking_numbers,
        fulfillments.VALUE:tracking_url as fulfillments_tracking_url,
        fulfillments.VALUE:tracking_urls as fulfillments_tracking_urls,
        fulfillments.VALUE:updated_at as fulfillments_updated_at,
        fulfillments_line_items.VALUE:id::VARCHAR as line_items_id,
        fulfillments_line_items.VALUE:admin_graphql_api_id::VARCHAR as line_items_admin_graphql_api_id,
        fulfillments_line_items.VALUE:fulfillable_quantity::VARCHAR as line_items_fulfillable_quantity,
        fulfillments_line_items.VALUE:fulfillment_service::VARCHAR as line_items_fulfillment_service,
        fulfillments_line_items.VALUE:gift_card::VARCHAR as line_items_gift_card,
        fulfillments_line_items.VALUE:grams::VARCHAR as line_items_grams, 
        fulfillments_line_items.VALUE:name::VARCHAR as line_items_name,
        fulfillments_line_items.VALUE:price::FLOAT as line_items_price,
        fulfillments_line_items.VALUE:price_set as line_items_price_set,
        fulfillments_line_items.VALUE:product_exists::VARCHAR as line_items_product_exists,
        fulfillments_line_items.VALUE:product_id::VARCHAR as line_items_product_id,
        fulfillments_line_items.VALUE:properties::VARCHAR as line_items_properties,
        fulfillments_line_items.VALUE:quantity::FLOAT as line_items_quantity,
        fulfillments_line_items.VALUE:requires_shipping::VARCHAR as line_items_requires_shipping,
        fulfillments_line_items.VALUE:sku::VARCHAR as line_items_sku,
        fulfillments_line_items.VALUE:taxable::VARCHAR as line_items_taxable,
        fulfillments_line_items.VALUE:title::VARCHAR as line_items_title,
        fulfillments_line_items.VALUE:total_discount::numeric as line_items_total_discount,
        fulfillments_line_items.VALUE:total_discount_set as line_items_total_discount_set,
        fulfillments_line_items.VALUE:variant_id::VARCHAR as line_items_variant_id,
        fulfillments_line_items.VALUE:variant_inventory_management::VARCHAR as line_items_variant_inventory_management,
        fulfillments_line_items.VALUE:variant_title::VARCHAR as line_items_variant_title,
        fulfillments_line_items.VALUE:tax_lines as line_items_tax_lines,
        fulfillments_line_items.VALUE:discount_allocations as line_items_discount_allocations,
        fulfillments_line_items.VALUE:pre_tax_price_set as line_items_pre_tax_price_set,
        fulfillments_line_items.VALUE:fulfillment_status::VARCHAR as line_items_fulfillment_status,
        fulfillments_line_items.VALUE:pre_tax_price as line_items_pre_tax_price,
        -- fulfillments_line_items.VALUE:tax_code as line_items_tax_code,
        fulfillments_line_items.VALUE:vendor::VARCHAR as line_items_vendor,
        fulfillments.VALUE:shipment_status as fulfillments_shipment_status,
        {% else %}
        COALESCE(cast(fulfillments.id as string),'') as fulfillments_id,
        fulfillments.admin_graphql_api_id as fulfillments_admin_graphql_api_id,
        fulfillments.created_at as fulfillments_created_at,
        fulfillments.location_id as fulfillments_location_id,
        fulfillments.name as fulfillments_name,
        cast(fulfillments.order_id as string) as fulfillments_orders_id,
        receipt.testcase as receipt_testcase,
        receipt.authorization as receipt_authorization,
        receipt.gift_cards as receipt_gift_cards,
        fulfillments.service as fulfillments_service,
        fulfillments.status as fulfillments_status,
        fulfillments.tracking_company as fulfillments_tracking_company,
        fulfillments.tracking_number as fulfillments_tracking_number,
        fulfillments.tracking_numbers as fulfillments_tracking_numbers,
        fulfillments.tracking_url as fulfillments_tracking_url,
        fulfillments.tracking_urls as fulfillments_tracking_urls,
        fulfillments.updated_at as fulfillments_updated_at,
        fulfillments_line_items.id as line_items_id,
        fulfillments_line_items.admin_graphql_api_id as line_items_admin_graphql_api_id,
        fulfillments_line_items.fulfillable_quantity as line_items_fulfillable_quantity,
        fulfillments_line_items.fulfillment_service as line_items_fulfillment_service,
        fulfillments_line_items.gift_card as line_items_gift_card,
        fulfillments_line_items.grams as line_items_grams, 
        fulfillments_line_items.name as line_items_name,
        cast(fulfillments_line_items.price as numeric) line_items_price,
        fulfillments_line_items.price_set as line_items_price_set,
        fulfillments_line_items.product_exists as line_items_product_exists,
        fulfillments_line_items.product_id as line_items_product_id,
        fulfillments_line_items.properties as line_items_properties,
        fulfillments_line_items.quantity as line_items_quantity,
        fulfillments_line_items.requires_shipping as line_items_requires_shipping,
        fulfillments_line_items.sku as line_items_sku,
        fulfillments_line_items.taxable as line_items_taxable,
        fulfillments_line_items.title as line_items_title,
        cast(fulfillments_line_items.total_discount as numeric) line_items_total_discount,
        fulfillments_line_items.total_discount_set as line_items_total_discount_set,
        fulfillments_line_items.variant_id as line_items_variant_id,
        fulfillments_line_items.variant_inventory_management as line_items_variant_inventory_management,
        fulfillments_line_items.variant_title as line_items_variant_title,
        fulfillments_line_items.tax_lines as line_items_tax_lines,
        fulfillments_line_items.discount_allocations as line_items_discount_allocations,
        fulfillments_line_items.pre_tax_price_set as line_items_pre_tax_price_set,
        fulfillments_line_items.fulfillment_status as line_items_fulfillment_status,
        fulfillments_line_items.pre_tax_price as line_items_pre_tax_price,
        -- fulfillments_line_items.tax_code as line_items_tax_code,
        fulfillments_line_items.vendor as line_items_vendor,
        fulfillments.shipment_status as fulfillments_shipment_status,
        {% endif %}
        a.line_items,
        payment_details,
        refunds,
        shipping_address,
        shipping_lines,
        app_id,
        customer_locale,
        note,
        closed_at,
        a.fulfillment_status,
        a.location_id,
        cancel_reason,
        cancelled_at,
        user_id,
        -- device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by a._daton_batch_runtime desc) row_num
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("FULFILLMENTS")}}
            {{multi_unnesting("FULFILLMENTS","RECEIPT")}}
            {% if target.type =='snowflake' %}
            , LATERAL FLATTEN( input => PARSE_JSON(fulfillments.VALUE:"line_items")) as fulfillments_line_items
            {% else %}
            left join unnest(fulfillments.line_items) as fulfillments_line_items
            {% endif %}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
