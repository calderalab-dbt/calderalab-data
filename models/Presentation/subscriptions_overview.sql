SELECT
  DISTINCT customer_id,
  subscription_id,
  created_at,
  cancelled_at,
  MIN(created_at) OVER (PARTITION BY customer_id) AS new_subscriber,
IF
  ( count ( created_at) OVER (PARTITION BY customer_id) - count ( cancelled_at) OVER (PARTITION BY customer_id) = 0, MAX(cancelled_at) OVER (PARTITION BY customer_id), NULL) cancelled_subscriber,
  CASE
    WHEN order_interval_unit = 'day' THEN CAST(order_interval_frequency AS int64)*1
    WHEN order_interval_unit = 'week' THEN CAST(order_interval_frequency AS int64)*7
    WHEN order_interval_unit = 'month' THEN CAST(order_interval_frequency AS int64)*30
END
  AS interval_days
FROM (
  SELECT
    *EXCEPT(cancelled_at),
    DATE(cancelled_at) cancelled_at
  FROM
    {{ ref('dim_subscription_recharge') }}
  WHERE
    created_at != "2023-10-10"
    OR status != 'cancelled'
    OR DATE(cancelled_at) IS NOT NULL)