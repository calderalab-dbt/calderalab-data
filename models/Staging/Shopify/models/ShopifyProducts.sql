{% if var('ShopifyProducts') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
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
{{set_table_name('%caldera_us_shopify_products%')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
    a.id,
    a.title,
    body_html,
    vendor,
    product_type,
    cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
    handle,
    cast(a.updated_at as {{ dbt.type_timestamp() }}) updated_at,
    published_at,
    published_scope,
    tags,
    a.admin_graphql_api_id,
    {% if target.type =='snowflake' %}
    variants.VALUE:id::VARCHAR as variant_id,
    coalesce(variants.VALUE:product_id::VARCHAR,'') as variants_product_id,
    variants.VALUE:title::VARCHAR as variants_title,
    variants.VALUE:price::numeric as variants_price,
    coalesce(variants.VALUE:sku::VARCHAR,'') as variants_sku,
    variants.VALUE:position::VARCHAR as variants_position,
    variants.VALUE:inventory_policy::VARCHAR as variants_inventory_policy,
    variants.VALUE:fulfillment_service::VARCHAR as variants_fulfillment_service,
    variants.VALUE:inventory_management::VARCHAR as variants_inventory_management,
    variants.VALUE:option1::VARCHAR as variants_option1,
    variants.VALUE:created_at::timestamp as variants_created_at,
    variants.VALUE:updated_at::timestamp as variants_updated_at,
    variants.VALUE:taxable::VARCHAR as variants_taxable,
    variants.VALUE:barcode::VARCHAR as variants_barcode,
    variants.VALUE:grams::VARCHAR as variants_grams,
    variants.VALUE:image_id::VARCHAR as variants_image_id,
    variants.VALUE:weight::numeric as variants_weight,
    variants.VALUE:weight_unit::VARCHAR as variants_weight_unit,
    variants.VALUE:inventory_item_id::VARCHAR as variants_inventory_item_id,
    variants.VALUE:inventory_quantity::numeric as variants_inventory_quantity,
    variants.VALUE:old_inventory_quantity::numeric as variants_old_inventory_quantity,
    variants.VALUE:presentment_prices as variants_presentment_prices,
    {% else %}
    variants.id as variant_id,
    coalesce(cast(variants.product_id as string),'') as variants_product_id,
    variants.title as variants_title,
    variants.price as variants_price,
    coalesce(cast(variants.sku as string),'') as variants_sku,
    variants.position as variants_position,
    variants.inventory_policy as variants_inventory_policy,
    variants.fulfillment_service as variants_fulfillment_service,
    variants.inventory_management as variants_inventory_management,
    variants.option1 as variants_option1,
    variants.created_at as variants_created_at,
    variants.updated_at as variants_updated_at,
    variants.taxable as variants_taxable,
    variants.barcode as variants_barcode,
    variants.grams as variants_grams,
    variants.image_id as variants_image_id,
    variants.weight as variants_weight,
    variants.weight_unit as variants_weight_unit,
    variants.inventory_item_id as variants_inventory_item_id,
    variants.inventory_quantity as variants_inventory_quantity,
    variants.old_inventory_quantity as variants_old_inventory_quantity,
    variants.presentment_prices as variants_presentment_prices,
    {% endif %}
    options,
    images,
    image,
    template_suffix,
    status,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    DENSE_RANK() OVER (PARTITION BY a.id order by _daton_batch_runtime desc) row_num
    from {{i}} a
            {{unnesting("variants")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}} and variants.sku is not null
            {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
