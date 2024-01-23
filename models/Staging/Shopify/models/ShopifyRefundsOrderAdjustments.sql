{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name_new('shopify_refunds_tbl_ptrn','%shopify_refunds','shopify_refunds_line_items_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}

{% for i in result %}
        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {{currency_conversion('c.value', 'c.from_currency_code', 'b.amount_set_presentment_money_currency_code') }},
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select 
        safe_cast(a.id as string) refund_id,
        safe_cast(a.order_id as string) order_id,
        {{timezone_conversion("a.created_at")}} as created_at,
        note,
        safe_cast(user_id as string) user_id,
        {{timezone_conversion("a.processed_at")}} as processed_at,
        restock,
        safe_cast(a.admin_graphql_api_id as string) admin_graphql_api_id,
        {{extract_nested_value('order_adjustments','id','string')}} as order_adjustments_id,
        {{extract_nested_value("order_adjustments","order_id","numeric")}} as order_adjustments_order_id,
        {{extract_nested_value("order_adjustments","refund_id","numeric")}} as order_adjustments_refund_id,
        {{extract_nested_value("order_adjustments","amount","numeric")}} as order_adjustments_amount,
        {{extract_nested_value("order_adjustments","tax_amount","numeric")}} as order_adjustments_tax_amount,
        {{extract_nested_value("order_adjustments","kind","string")}} as order_adjustments_kind,
        {{extract_nested_value("order_adjustments","reason","string")}} as order_adjustments_reason,
        {{extract_nested_value('amount_set_presentment_money','amount','string')}} as amount_set_presentment_money_amount,
        {{extract_nested_value('amount_set_presentment_money','currency_code','string')}} as amount_set_presentment_money_currency_code,
        {{extract_nested_value('tax_amount_set_presentment_money','amount','string')}} as tax_amount_set_presentment_money_amount,
        {{extract_nested_value('tax_amount_set_presentment_money','currency_code','string')}} as tax_amount_set_presentment_money_currency_code,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("order_adjustments")}}
            {{multi_unnesting("order_adjustments","amount_set")}}
            {{multi_unnesting_new("amount_set","presentment_money")}}
            {{multi_unnesting("order_adjustments","tax_amount_set")}}
            {{multi_unnesting_new("tax_amount_set","presentment_money")}}
            

            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_refunds_line_items_lookback',2592000000) }},0) from {{ this }})
            {% endif %}

        qualify row_number() over (partition by a.id, a.order_id, {{extract_nested_value('order_adjustments','id','string')}} order by _daton_batch_runtime desc)=1

) b
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.amount_set_presentment_money_currency_code = c.to_currency_code
        {% endif %}
    

    {% if not loop.last %} union all {% endif %}
{% endfor %}