SELECT
  *EXCEPT(cancelled_at),
  DATE(cancelled_at) cancelled_at
FROM
  {{ ref('dim_subscription_recharge') }}
WHERE
  created_at != "2023-10-10"
  OR status != 'cancelled'
  OR DATE(cancelled_at) IS NOT NULL