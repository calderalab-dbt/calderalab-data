{% if var('ShopifyCustomers') %}
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
{{set_table_name('%caldera_us_shopify_customers%')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
        COALESCE(cast(a.id as string),'') as customers_id,
        email,
        accepts_marketing,
        CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
        CAST({{ timezone_conversion("a.updated_at") }} AS TIMESTAMP) as updated_at,
        a.first_name,
        a.last_name,
        orders_count,
        a.state,
        total_spent,
        verified_email,
        tax_exempt,
        tags,
        currency,
        a.phone,
        addresses,
        accepts_marketing_updated_at,
        a.admin_graphql_api_id,
        {% if target.type =='snowflake' %}
        default_address.VALUE:id::VARCHAR as default_address_id,
        default_address.VALUE:customer_id::VARCHAR as default_address_customer_id,
        default_address.VALUE:first_name::VARCHAR as default_address_first_name,
        default_address.VALUE:last_name::VARCHAR as default_address_last_name,
        default_address.VALUE:address1::VARCHAR as default_address_address1,
        default_address.VALUE:city::VARCHAR as default_address_city,
        default_address.VALUE:province::VARCHAR as default_address_province,
        default_address.VALUE:country::VARCHAR as default_address_country,
        default_address.VALUE:zip::VARCHAR as default_address_zip,
        default_address.VALUE:phone::VARCHAR as default_address_phone,
        default_address.VALUE:name::VARCHAR as default_address_name,
        default_address.VALUE:province_code::VARCHAR as default_address_province_code,
        default_address.VALUE:country_code::VARCHAR as default_address_country_code,
        default_address.VALUE:country_name::VARCHAR as default_address_country_name,
        default_address.VALUE:default::VARCHAR as default_address_default,
        default_address.VALUE:address2::VARCHAR as default_address_address2,
        default_address.VALUE:company::VARCHAR as default_address_company,
        {% else %}
        default_address.id as default_address_id,
        default_address.customer_id as default_address_customer_id,
        default_address.first_name as default_address_first_name,
        default_address.last_name as default_address_last_name,
        default_address.address1 as default_address_address1,
        default_address.city as default_address_city,
        default_address.province as default_address_province,
        default_address.country as default_address_country,
        default_address.zip as default_address_zip,
        default_address.phone as default_address_phone,
        default_address.name as default_address_name,
        default_address.province_code as default_address_province_code,
        default_address.country_code as default_address_country_code,
        default_address.country_name as default_address_country_name,
        default_address.default as default_address_default,
        default_address.address2 as default_address_address2,
        default_address.company as default_address_company,
        {% endif %}
        email_marketing_consent,
        last_order_id,
        last_order_name,
        marketing_opt_in_level,
        note,
        sms_marketing_consent,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by {{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {{unnesting("default_address")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
