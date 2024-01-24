WITH base AS (
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
  `caldera-labs.dev_edm_prep.fact_order_lines_shopify` o
LEFT JOIN
  `dev_edm_main.dim_product` p
ON
    to_hex(MD5(CAST(COALESCE(CAST(o.product_id AS string ), '') || '-' || COALESCE(CAST(o.sku AS string ), '') || '-' || COALESCE(CAST(o.platform_name AS string ), '') AS string ))) = p.product_key
LEFT JOIN   
   `dev_edm_prep.dim_subscription_recharge` s
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
    `caldera-labs`.`dev_edm_main`.`fact_orders` 
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
    `caldera-labs`.`dev_edm_main`.`fact_orders`
  WHERE
    transaction_type = 'Return'
  GROUP BY
    1 )

SELECT ord.*, IFNULL(ref.refunds,0)refunds, base.*except(date) 
FROM ord
LEFT JOIN ref
ON ord.date = ref.date
LEFT JOIN base
ON ord.date = base.date
ORDER BY 1 DESC