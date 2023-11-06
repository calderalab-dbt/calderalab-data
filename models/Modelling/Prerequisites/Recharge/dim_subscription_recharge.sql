
SELECT * {{exclude()}}(row_num) from (
    select 
    brand,
    'Shopify' as platform_name,
    'Recharge' as order_channel,
    {{store_name('store')}},
    subscription_id,
    customer_id,
    utm_source,
    utm_medium,
    date(created_at) created_at,
    external_product_id,
    external_variant_id,
    next_charge_scheduled_at,
    cast(order_interval_frequency as string) order_interval_frequency,
    cast(order_interval_unit as string) order_interval_unit,
    product_title,
    quantity,
    sku,
    status,
    date(updated_at) as updated_at,
    cancellation_reason,
    cancelled_at,
    order_day_of_month,
    presentment_currency,
    cancellation_reason_comments,
    row_number() over(partition by external_product_id, sku, subscription_id, cancelled_at order by _daton_batch_runtime desc) row_num
    from {{ ref('RechargeSubscriptions') }}
) where row_num = 1