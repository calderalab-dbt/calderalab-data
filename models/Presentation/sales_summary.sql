{{ config(
    materialized='table'
)}}

WITH product_base AS (
  SELECT
  date,
  sum(case when product_name = 'The Good' then o.quantity end) The_Good_Units,
  sum(case when product_name = 'The Base Layer' then o.quantity end) The_Base_Layer_Units,
  sum(case when product_name = 'The Clean Slate' then o.quantity end) The_Clean_Slate_Units,
  sum(case when product_name = 'The Eyecon' then o.quantity end) The_Eyecon_Units,
  sum(case when product_name = 'The Deep' then o.quantity end) The_Deep_Units,
  sum(case when product_name = 'The Face SPF' then o.quantity end) The_Face_SPF_Units,
  sum(case when product_name = 'The Body Bar' then o.quantity end) The_Body_Bar_Units,
  sum(case when product_name = 'The Beard' then o.quantity end) The_Beard_Units,
  sum(case when product_name = 'The Smooth' then o.quantity end) The_Smooth_Units,
  count(case when s.subscription_id is not null and product_name = 'The Good' then o.order_id end) The_Good_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Base Layer' then o.order_id end) The_Base_Layer_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Clean Slate' then o.order_id end) The_Clean_Slate_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Eyecon' then o.order_id end) The_Eyecon_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Deep' then o.order_id end) The_Deep_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Face SPF' then o.order_id end) The_Face_SPF_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Body Bar' then o.order_id end) The_Body_Bar_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Beard' then o.order_id end) The_Beard_Sub_Orders,
  count(case when s.subscription_id is not null and product_name = 'The Smooth' then o.order_id end) The_Smooth_Sub_Orders,
FROM
  {{ref('fact_order_lines_shopify')}} o
LEFT JOIN
  {{ref('dim_product')}} p
ON
    to_hex(MD5(CAST(COALESCE(CAST(o.product_id AS string ), '') || '-' || COALESCE(CAST(o.sku AS string ), '') || '-' || COALESCE(CAST(o.platform_name AS string ), '') AS string ))) = p.product_key
LEFT JOIN   
   {{ref('dim_subscription_recharge')}} s
ON
  o.subscription_id = s.subscription_id
WHERE
  is_cancelled IS FALSE
  AND transaction_type = 'Order'
GROUP by 1
),

  ord AS (
  SELECT
    date,
    SUM(IFNULL(subtotal_price,0)) gross_sales,
    SUM(IFNULL(order_discount,0)) discounts,
    SUM(IFNULL(shipping_price,0)) - SUM(IFNULL(shipping_discount,0)) shipping_price,
    SUM(IFNULL(total_tax,0)) total_taxes,
  FROM
    {{ref('fact_orders')}}
  WHERE
    transaction_type = 'Order'
    AND is_cancelled = FALSE
  GROUP BY
    1),

  ref AS (
  SELECT
    date,
    SUM(total_price) refunds
  FROM
    {{ref('fact_orders')}}
  WHERE
    transaction_type = 'Return'
  GROUP BY
    1 ),

first_customer as (
  SELECT
  DISTINCT customer_id,
  FIRST_VALUE(order_id) OVER (PARTITION BY customer_id ORDER BY date) first_order_id,
  MIN(date) OVER (PARTITION BY customer_id) first_order_date
FROM
  {{ref('fact_orders_shopify')}}
),

customer_base as (
SELECT
  o.date,
  order_id,
  o.customer_id,
  CASE WHEN LOWER(tags) LIKE '%subscription%' THEN 'Subscription' ELSE 'One-time' END purchase_option,
  CASE WHEN o.order_id = c.first_order_id THEN 'First-time' ELSE 'Returning'  END customer_type,
  ifnull(subtotal_price,0) - ifnull(order_discount,0) gsld
FROM
  {{ref('fact_orders_shopify')}} o
LEFT JOIN first_customer c
ON o.order_id = c.first_order_id
WHERE is_cancelled IS FALSE AND transaction_type = 'Order'
),

customer_final as (
SELECT 
date,
count(distinct order_id) total_orders,
count(distinct customer_id) total_customers,
count(case when customer_type = 'First-time' then order_id end) new_customer_orders,
count(case when customer_type = 'Returning' then order_id end) existing_customer_orders,
count(case when purchase_option = 'One-time' then order_id end) one_time_orders,
count(case when purchase_option = 'Subscription' then order_id end) subscription_orders,
count(case when (customer_type = 'First-time' and purchase_option = 'Subscription') or (purchase_option = 'One-time') then order_id end) acquisition_orders,
count(case when customer_type = 'Returning' and purchase_option = 'Subscription' then order_id end) recurring_orders,
sum(case when customer_type = 'First-time' then gsld end) new_customer_gsld,
sum(case when customer_type = 'Returning' then gsld end) existing_customer_gsld,
sum(case when purchase_option = 'One-time' then gsld end) one_time_gsld,
sum(case when purchase_option = 'Subscription' then gsld end) subscription_gsld,
sum(case when (customer_type = 'First-time' and purchase_option = 'Subscription') or (purchase_option = 'One-time') then gsld end) acquisition_gsld,
sum(case when customer_type = 'Returning' and purchase_option = 'Subscription' then gsld end) recurring_gsld,
count(case when customer_type = 'First-time' and purchase_option = 'Subscription' then order_id end) new_customer_sub_orders,
count(case when customer_type = 'First-time' and purchase_option = 'One-time' then order_id end) new_customer_otp_orders,
count(case when customer_type = 'Returning' and purchase_option = 'Subscription' then order_id end) existing_customer_sub_orders,
count(case when customer_type = 'Returning' and purchase_option = 'One-time' then order_id end) existing_customer_otp_orders,
FROM
  customer_base 
GROUP BY 1
ORDER BY 1 DESC
)

SELECT ord.*, IFNULL(ref.refunds,0)refunds, product_base.*except(date), customer_final.*except(date)
FROM ord
LEFT JOIN ref
ON ord.date = ref.date
LEFT JOIN product_base
ON ord.date = product_base.date
LEFT JOIN customer_final
ON ord.date = customer_final.date
ORDER BY 1 DESC