{% if var('ShopifyRefundsTransactions') %}
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

with unnested_refunds as(
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

    SELECT * 
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then b.transactions_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            b.transactions_currency as exchange_currency_code, 
        {% endif %}
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        CAST(a.id as string) refund_id,
        a.order_id,
        a.created_at,
        note,
        a.user_id,
        a.processed_at,
        restock,
        a.admin_graphql_api_id,
        refund_line_items,
        {% if target.type =='snowflake' %}
            COALESCE(transactions.VALUE:id::VARCHAR,'') as transactions_id,
            transactions.VALUE:order_id::VARCHAR as transactions_order_id,
            transactions.VALUE:kind::VARCHAR as transactions_kind,
            transactions.VALUE:gateway::VARCHAR as transactions_gateway,
            transactions.VALUE:status::VARCHAR as transactions_status,
            transactions.VALUE:created_at::VARCHAR as transactions_created_at,
            transactions.VALUE:test::VARCHAR as transactions_test,
            transactions.VALUE:authorization::VARCHAR as transactions_authorization,
            transactions.VALUE:parent_id::VARCHAR as transactions_parent_id,
            transactions.VALUE:processed_at::TIMESTAMP as transactions_processed_at,
            transactions.VALUE:source_name::VARCHAR as transactions_source_name,
            transactions.VALUE:amount as transactions_amount,
            transactions.VALUE:currency as transactions_currency,
            transactions.VALUE:admin_graphql_api_id as transactions_admin_graphql_api_id,
            transactions.VALUE:payment_details as transactions_payment_details,
            transactions.VALUE:receipt::VARCHAR as transactions_receipt,
            transactions.VALUE:payments_refund_attributes as transactions_payments_refund_attributes,
            transactions.VALUE:message as transactions_message,
            transactions.VALUE:user_id as transactions_user_id,
            transactions.VALUE:payment_id as transactions_payment_id,
            transactions.VALUE:error_code::VARCHAR as transactions_error_code,
        {% else %}
            COALESCE(CAST(transactions.id as string),'') as transactions_id,
            transactions.order_id as transactions_order_id,
            transactions.kind as transactions_kind,
            transactions.gateway as transactions_gateway,
            transactions.status as transactions_status,
            transactions.created_at as transactions_created_at,
            transactions.test as transactions_test,
            transactions.authorization as transactions_authorization,
            transactions.parent_id as transactions_parent_id,
            transactions.processed_at as transactions_processed_at,
            transactions.source_name as transactions_source_name,
            transactions.amount as transactions_amount,
            transactions.currency as transactions_currency,
            transactions.admin_graphql_api_id as transactions_admin_graphql_api_id,
            transactions.payment_details as transactions_payment_details,
            transactions.receipt as transactions_receipt,
            transactions.payments_refund_attributes as transactions_payments_refund_attributes,
            transactions.message as transactions_message,
            transactions.user_id as transactions_user_id,
            transactions.payment_id as transactions_payment_id,
            transactions.error_code as transactions_error_code,
        {% endif %}
        total_duties_set,
        order_adjustments,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("transactions")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            ) b 
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.transactions_currency = c.to_currency_code
            {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY refund_id order by _daton_batch_runtime desc) row_num
from unnested_refunds 
)

SELECT *, ROW_NUMBER() OVER (PARTITION BY refund_id order by _daton_batch_runtime desc) _seq_id
from (
select * {{exclude()}} (row_num)
from dedup 
where row_num = 1)
