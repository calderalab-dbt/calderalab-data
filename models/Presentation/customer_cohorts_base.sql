{{ config(
    materialized='table'
)}}

with first_customer as (
  SELECT
  DISTINCT customer_id,
  FIRST_VALUE(order_id) OVER (PARTITION BY customer_id ORDER BY date) first_order_id,
  MIN(date) OVER (PARTITION BY customer_id) first_order_date
FROM {{ ref('fact_orders_shopify')}}
)
,

order_base as (
SELECT
  o.date order_date,
  first_order_date aquisition_date,
  left(string(date),7) order_month,
  left(string(first_order_date),7) aquisition_month,
  order_id,
  o.customer_id,
  CASE WHEN LOWER(tags) LIKE '%subscription%' THEN 'Subscription' ELSE 'One-time' END purchase_option,
  CASE WHEN left(string(date),7) = left(string(first_order_date),7) THEN 'First-time' ELSE 'Returning'  END customer_type,
  -- CASE WHEN o.order_id = c.first_order_id THEN 'First-time' ELSE 'Returning'  END customer_type,
  ifnull(subtotal_price,0) - ifnull(order_discount,0) gsld
  FROM {{ ref('fact_orders_shopify')}} o
LEFT JOIN first_customer c
ON o.customer_id = c.customer_id
WHERE is_cancelled IS FALSE AND transaction_type = 'Order'
order by customer_id
)


SELECT 
left(string(aquisition_date),7) acquisition_month,
purchase_option as aquisition_ordertype,
left(string(order_date),7) order_month,
((cast(substr(order_month,1,4)as INT64)-cast(substr(aquisition_month,1,4) as INT64))*12 +
cast(substr(order_month,6,2)as INT64)-cast(substr(aquisition_month,6,2) as INT64)) as Month_Diff,
count (distinct case when customer_type = 'First-time' then customer_id end) new_customers,
count (distinct case when customer_type = 'Returning' then customer_id end) repeat_customers,
count (distinct order_id) total_orders,
sum (gsld) revenue,
count (distinct customer_id) total_customers,
from order_base a
where aquisition_date is not null
group by 1,2,3,4
order by 1 desc, 3 desc