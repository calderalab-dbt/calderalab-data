{% if var('ShopifyOrdersShippingLines') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
    {%- set max_loaded_query -%}
    select coalesce(max(_daton_batch_runtime) - 2592000000, 0) from {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
        {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
        {% set max_loaded = 0 %}
    {%- endif -%}
{% endif %}

{% set table_name_query %}
    {{ set_table_name('%caldera_us_shopify_orders%') }} and lower(table_name) not like '%shopify%fulfillment_orders'
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
        {% set brand = i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store = i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}


    select 
        '{{ brand }}' as brand,
        '{{ store }}' as store,
        safe_cast(a.id as string) as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        safe_cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        CAST({{ timezone_conversion("a.created_at") }} AS TIMESTAMP) as created_at,
        currency,
        {{extract_nested_value("discount_codes","code","string")}} as discount_code,
        {{extract_nested_value("discount_codes","amount","numeric")}} as discount_amount,
        {{extract_nested_value("discount_codes","type","string")}} as discount_type,
        email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        name,
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
        tags,
        taxes_included,
        test,
        token,
        CAST({{ timezone_conversion("a.updated_at") }} AS TIMESTAMP) as updated_at,
        coalesce({{extract_nested_value("shipping_lines","id","string")}},'N/A') as shipping_lines_id,
        {{extract_nested_value("shipping_lines","code","string")}} as shipping_lines_code,
        {{extract_nested_value("shipping_lines","discounted_price","numeric")}} as shipping_lines_discounted_price,
        {{extract_nested_value("shipping_lines","price","numeric")}} as shipping_lines_price,
        {{extract_nested_value("shipping_lines","source","string")}} as shipping_lines_source,
        {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,
        {{extract_nested_value("shipping_lines","carrier_identifier","string")}} as shipping_lines_carrier_identifier,
        {{extract_nested_value("discount_allocations","amount","numeric")}} as shipping_lines_discount_amount,
        safe_cast(app_id as string) as app_id,
        customer_locale,
        note,
        datetime(safe_cast(closed_at as timestamp),'{{ var('to_timezone') }}') closed_at,
        fulfillment_status,
        safe_cast(location_id as string) as location_id,
        cancel_reason,
        datetime(safe_cast(cancelled_at as timestamp),'{{ var('to_timezone') }}') cancelled_at,
        safe_cast(user_id as string) as user_id,
        -- safe_cast(device_id as string) as device_id,
    {% if var('currency_conversion_flag') %}
        case when b.value is null then 1 else b.value end as exchange_currency_rate,
        case when b.from_currency_code is null then currency else b.from_currency_code end as exchange_currency_code,
    {% else %}
        safe_cast(1 as decimal) as exchange_currency_rate,
        currency as exchange_currency_code,
    {% endif %}
        a.{{ daton_user_id() }} as _daton_user_id,
        a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
        a.{{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a
        {% if var('currency_conversion_flag') %}
            left join {{ ref('ExchangeRates') }} b on date(created_at) = b.date and currency = b.to_currency_code
        {% endif %}
        {{ unnesting("discount_codes") }}
        {{ unnesting("shipping_lines") }}
        {{ multi_unnesting("shipping_lines", "discount_allocations") }}

        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{ daton_batch_runtime() }} >= {{ max_loaded }}
        {% endif %}

    qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}