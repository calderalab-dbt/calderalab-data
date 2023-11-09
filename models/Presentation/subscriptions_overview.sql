select 
distinct
-- b.brand_name,
c.platform_name,
e.order_channel,
c.store_name,
coalesce(product_id,'') product_id,
coalesce(product_name,'') product_name,
coalesce(sku,'') sku,
subscription_id,
customer_id,
utm_source,
utm_medium,
created_at,
next_charge_scheduled_at,
order_interval_frequency,
order_interval_unit,
cancellation_reason,
cancelled_at,
order_day_of_month,
presentment_currency,
cancellation_reason_comments
from {{ ref('fact_order_lines')}} a

left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key 
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} ) d
on a.product_key = d.product_key
left join (select * from {{ ref('dim_subscription')}} where status = 'Active') e
on a.subscription_key = e.subscription_key 


