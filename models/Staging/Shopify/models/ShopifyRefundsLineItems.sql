{% if var('ShopifyRefundsLineItems') %}
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
{{set_table_name('%caldera_us_shopify_refunds%')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then b.subtotal_set_presentment_currency_code else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            b.subtotal_set_presentment_currency_code as exchange_currency_code, 
        {% endif %}
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        CAST(a.id as string) refund_id,
        order_id,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        note,
        user_id,
        CAST(a.processed_at as timestamp) processed_at,
        restock,
        a.admin_graphql_api_id,
        {% if target.type =='snowflake' %}
        COALESCE(refund_line_items.VALUE:id::VARCHAR,'') as refund_line_items_id,
        refund_line_items.VALUE:quantity::NUMERIC as refund_line_items_quantity,
        COALESCE(refund_line_items.VALUE:line_item_id::VARCHAR,'') as refund_line_items_line_item_id,
        refund_line_items.VALUE:location_id::VARCHAR as refund_line_items_location_id,
        refund_line_items.VALUE:restock_type::VARCHAR as refund_line_items_restock_type,
        refund_line_items.VALUE:subtotal::NUMERIC as refund_line_items_subtotal,
        refund_line_items.VALUE:total_tax::NUMERIC as refund_line_items_total_tax,
        presentment_money.VALUE:amount as subtotal_set_presentment_amount,
        presentment_money.VALUE:currency_code as subtotal_set_presentment_currency_code,
        shop_money.VALUE:amount as subtotal_set_shop_amount,
        shop_money.VALUE:currency_code as subtotal_set_shop_currency_code,
        refund_line_items.VALUE:total_tax_set as refund_line_items_total_tax_set,
        line_item.VALUE:id::VARCHAR as line_item_id,
        COALESCE(line_item.VALUE:variant_id::VARCHAR,'') as line_item_variant_id,
        line_item.VALUE:title::VARCHAR as line_item_title,
        line_item.VALUE:quantity::NUMERIC as line_item_quantity,
        line_item.VALUE:sku::VARCHAR as line_item_sku,
        line_item.VALUE:variant_title::VARCHAR as line_item_variant_title,
        line_item.VALUE:fulfillment_service::VARCHAR as line_item_fulfillment_service,
        line_item.VALUE:product_id::VARCHAR as line_item_product_id,
        line_item.VALUE:requires_shipping::VARCHAR as line_item_requires_shipping,
        line_item.VALUE:taxable::VARCHAR as line_item_taxable,
        line_item.VALUE:gift_card::VARCHAR as line_item_gift_card,
        line_item.VALUE:name::VARCHAR as line_item_name,
        line_item.VALUE:variant_inventory_management::VARCHAR as line_item_variant_inventory_management,
        line_item.VALUE:product_exists::VARCHAR as line_item_product_exists,
        line_item.VALUE:fulfillable_quantity::NUMERIC as line_item_fulfillable_quantity,
        line_item.VALUE:grams::VARCHAR as line_item_grams,
        line_item.VALUE:price::NUMERIC as line_item_price,
        line_item.VALUE:total_discount::NUMERIC as line_item_total_discount,
        line_item.VALUE:price_set as line_item_price_set,
        line_item.VALUE:total_discount_set as line_item_total_discount_set,
        line_item.VALUE:discount_allocations as line_item_discount_allocations,
        line_item.VALUE:admin_graphql_api_id as line_item_admin_graphql_api_id,
        line_item.VALUE:properties as line_item_properties,
        line_item.VALUE:tax_lines as line_item_tax_lines,
        line_item.VALUE:pre_tax_price_set as line_item_pre_tax_price_set,
        line_item.VALUE:vendor as line_item_vendor,
        -- line_item.VALUE:tax_code as line_item_tax_code,
        line_item.VALUE:pre_tax_price::NUMERIC as line_item_pre_tax_price,
        line_item.VALUE:fulfillment_status::VARCHAR as line_item_fulfillment_status,
        line_item.VALUE:origin_location::VARCHAR as line_item_origin_location,
        line_item.VALUE:destination_location::VARCHAR as line_item_destination_location,        
        {% else %}
        COALESCE(CAST(refund_line_items.id as string),'') as refund_line_items_id,
        refund_line_items.quantity as refund_line_items_quantity,
        COALESCE(CAST(refund_line_items.line_item_id as string),'') as refund_line_items_line_item_id,
        refund_line_items.location_id as refund_line_items_location_id,
        refund_line_items.restock_type as refund_line_items_restock_type,
        refund_line_items.subtotal as refund_line_items_subtotal,
        refund_line_items.total_tax as refund_line_items_total_tax,
        presentment_money.amount as subtotal_set_presentment_amount,
        presentment_money.currency_code as subtotal_set_presentment_currency_code,
        shop_money.amount as subtotal_set_shop_amount,
        shop_money.currency_code as subtotal_set_shop_currency_code,
        refund_line_items.total_tax_set as refund_line_items_total_tax_set,
        line_item.id as line_item_id,
        COALESCE(CAST(line_item.variant_id as string),'') as line_item_variant_id,
        line_item.title as line_item_title,
        line_item.quantity as line_item_quantity,
        line_item.sku as line_item_sku,
        line_item.variant_title as line_item_variant_title,
        line_item.fulfillment_service as line_item_fulfillment_service,
        line_item.product_id as line_item_product_id,
        line_item.requires_shipping as line_item_requires_shipping,
        line_item.taxable as line_item_taxable,
        line_item.gift_card as line_item_gift_card,
        line_item.name as line_item_name,
        line_item.variant_inventory_management as line_item_variant_inventory_management,
        line_item.product_exists as line_item_product_exists,
        line_item.fulfillable_quantity as line_item_fulfillable_quantity,
        line_item.grams as line_item_grams,
        line_item.price as line_item_price,
        line_item.total_discount as line_item_total_discount,
        line_item.price_set as line_item_price_set,
        line_item.total_discount_set as line_item_total_discount_set,
        line_item.discount_allocations as line_item_discount_allocations,
        line_item.admin_graphql_api_id as line_item_admin_graphql_api_id,
        line_item.tax_lines as line_item_tax_lines,
        line_item.properties as line_item_properties,
        line_item.pre_tax_price_set as line_item_pre_tax_price_set,
        line_item.vendor as line_item_vendor,
        -- line_item.tax_code as line_item_tax_code,
        line_item.pre_tax_price as line_item_pre_tax_price,
        line_item.fulfillment_status as line_item_fulfillment_status,
        line_item.origin_location as line_item_origin_location,
        line_item.destination_location as line_item_destination_location,
        {% endif %}
        transactions,
        total_duties_set,
        order_adjustments,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY a.id, refund_line_items.VALUE:id, line_item.VALUE:variant_id order by _daton_batch_runtime desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY a.id, refund_line_items.id, line_item.variant_id order by _daton_batch_runtime desc) row_num
        {% endif %}
        from {{i}} a
            {{unnesting("refund_line_items")}}
            {{multi_unnesting("refund_line_items","line_item")}}
            {{multi_unnesting("refund_line_items","subtotal_set")}}
            {{multi_unnesting("subtotal_set","shop_money")}}
            {{multi_unnesting("subtotal_set","presentment_money")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}

        ) b
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.subtotal_set_presentment_currency_code = c.to_currency_code
        {% endif %}
    )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
