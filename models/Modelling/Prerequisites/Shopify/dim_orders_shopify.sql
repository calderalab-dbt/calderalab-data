
{% if var('recharge_flag') %}
-- depends_on: {{ ref('RechargeOrdersLineItemsProperties') }}
{% endif %}

{% if var('upscribe_flag') %}
-- depends_on: {{ ref('UpscribeSubscriptionItems') }}
{% endif %}


select * {{exclude()}} (row_num) from 
    (
    select
    'Shopify' as platform_name,
    ord.order_id,
    coalesce(customer_id,'') as customer_id,
    payment_gateway_names as payment_mode,
    {% if var('recharge_flag') %}
    recharge.order_channel,
    {% elif var('upscribe_flag') %}
    upscribe.order_channel,
    {% else %}
    --'Online Store' as order_channel,
    source_name as order_channel,
    {% endif %}
    date(ord.updated_at) as last_updated_date,
    row_number() over(partition by ord.order_id order by date(ord.updated_at) desc) row_num
    from {{ ref('ShopifyOrdersCustomer') }} ord

    {% if var('recharge_flag') %}
    left join (
    select distinct 'Recharge' as order_channel, 
    external_order_id as order_id
    from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
    on ord.order_id = recharge.order_id

    {% elif upscribe_flag %}
    left join (
    select distinct 'Upscribe' as order_channel, 
    cast(shopify_order_id as string) as order_id,
    from {{ ref('UpscribeSubscriptionItems') }}) upscribe 
    on ord.order_id = upscribe.order_id
    {% endif %}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(ord.updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ord.updated_at)") }}
    {% endif %}
    )
where row_num = 1


