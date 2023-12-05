{% if var('ShopifyTransactions') %}
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
{{set_table_name('%caldera_us_shopify_transactions%')}} 
and lower(table_name) not like '%tender%' and lower(table_name) not like '%balance%' and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
    id,
    order_id,
    kind,
    gateway,
    status,
    CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
    test,
    authorization,
    parent_id,
    CAST({{ timezone_conversion("a.processed_at") }} AS TIMESTAMP) as processed_at,
    source_name,
    amount,
    currency,
    payment_id,
    total_unsettled_set,
    admin_graphql_api_id,
    payment_details,
    receipt,
    message,
    error_code,
    payments_refund_attributes,
    user_id,
    -- location_id,
    -- device_id,
    -- signature,
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
    ROW_NUMBER() OVER (PARTITION BY id order by a.{{daton_batch_runtime()}} desc) row_num
	    from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.processed_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
