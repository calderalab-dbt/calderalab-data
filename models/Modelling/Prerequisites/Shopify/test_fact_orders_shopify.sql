select
ord_ln_itms.order_id,
ord_ln_itms.brand,
'Shopify' as platform_name,
{{store_name('ord_ln_itms.store')}},
currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,

'Order' as transaction_type,
coalesce(info.customer_id,'') as customer_id,
coalesce(info.email,'') as email,
false as is_cancelled,
cast(null as boolean) as is_business_order,
sum(order_quantity) quantity,
sum(cast(order_price AS numeric) + ifnull(CAST(total_tax AS numeric),0)) total_price,
sum(cast(order_price AS numeric)) subtotal_price,
sum(cast(total_tax AS numeric)) total_tax,
sum(cast(order_shipping_price as numeric)) as shipping_price,
cast(null as numeric) as giftwrap_price,
sum(cast(total_discounts AS numeric)) as order_discount,
-- sum(coalesce(cast(discount_amount AS numeric),0)) as order_discount,
sum(cast(null as numeric)) as shipping_discount
-- customer_id
-- from
-- ( select *
from {{ ref('ShopifyOrders') }} ord
left join (select email, customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }}
where email is not null
qualify row_number() over(partition by order_id order by date(updated_at) desc)=1
) info
on ord.order_id = info.order_id
left join
(select order_id,brand, store, sum(cast(line_items_quantity as numeric)) order_quantity, sum(CAST(line_items_price AS numeric)*line_items_quantity) order_price from {{ref('ShopifyOrdersLineItems')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
{% endif %}
group by 1,2,3) ord_ln_itms
on ord.order_id=ord_ln_itms.order_id and ord.brand = ord_ln_itms.brand and ord.store = ord_ln_itms.store
left join (select order_id,brand,sum(cast(shipping_lines_price as numeric)) order_shipping_price from {{ref('ShopifyOrdersShippingLines')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
{% endif %}
group by 1,2) ord_shp_lns
on ord.order_id=ord_shp_lns.order_id and ord.brand = ord_shp_lns.brand
-- fetching the subscription ids in case of recharge orders

 
-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel,
  cast(shopify_order_id as string) as order_id,
--   items_sku as sku,
--   subscription_id
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ord.order_id= upscribe.order_id and ord.line_items_sku = upscribe.sku
{% endif %}
group by 1,2,3,4,5,6,7,8,10,11,12
 
UNION ALL
 
select
cast(ord_rfnd_ln_itms.order_id as string) as order_id,
brand,
'Shopify' as platform_name,
{{store_name('store')}},
rfnd_tran.transactions_currency as currency,
exg.to_currency_code as exchange_currency_code,
coalesce(exg.value,1) as exchange_currency_rate,
date(ord_rfnd_ln_itms.refund_date) as date,

'Return' as transaction_type,
coalesce(info.customer_id,'') as customer_id,
coalesce(info.email,'') as email,
false as is_cancelled,
cast(null as boolean) as is_business_order,
sum(cast(return_quantity as numeric)) quantity,
sum(coalesce(refund_amount,0) +
case when coalesce(transactions_amount,0)=0 then 0 else (coalesce(cast(transactions_amount as numeric),0) - coalesce(refund_amount,0)) end
+ coalesce(adjustments_amount,0)) as total_price,
sum(coalesce(refund_amount,0) +
case when coalesce(transactions_amount,0)=0 then 0 else (coalesce(cast(transactions_amount as numeric),0) - coalesce(refund_amount,0) - coalesce(cast(refund_tax as numeric),0)) end
+ coalesce(adjustments_amount,0) - coalesce(gift_amount,0)) as subtotal_price,
sum(cast(refund_tax as numeric)) total_tax,
sum(coalesce(adjustments_amount,0)) as shipping_price,
cast(null as numeric) as giftwrap_price,
sum(cast(refund_discount as numeric)) order_discount,
cast(null as numeric) as shipping_discount
-- cast(null as string) as customer_id
from
(select brand, store, cast(order_id as string) as order_id, date(created_at) as refund_date,
sum(coalesce(refund_line_items_quantity,0)) as refund_quantity,
sum(coalesce(cast(subtotal_set_presentment_amount as numeric),0)) as refund_amount,
sum(coalesce(cast(line_item_total_discount as numeric),0)) refund_discount,
sum(case when line_item_gift_card then refund_line_items_subtotal end ) gift_amount
from {{ref('ShopifyRefundsLineItems')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1,2,3,4) ord_rfnd_ln_itms
left join
(select transactions_currency, exchange_currency_code, order_id, date(created_at) as refund_date,
sum(coalesce(cast(transactions_amount as numeric),0)) transactions_amount from {{ ref('ShopifyRefundsTransactions')}}
where
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }} and
{% endif %}
transactions_amount is not null and transactions_status='success' group by 1,2,3,4) rfnd_tran
on cast(rfnd_tran.order_id as string)=ord_rfnd_ln_itms.order_id and rfnd_tran.refund_date=ord_rfnd_ln_itms.refund_date
left join
(select amount_set_presentment_money_currency_code, order_id,
date(created_at) as refund_date,
sum(coalesce(cast(amount_set_presentment_money_amount as numeric),0)) adjustments_amount
from {{ ref('ShopifyRefundsOrderAdjustments')}}
where order_adjustments_kind!='refund_discrepancy'
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    and date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1,2,3) rfnd_adj
on cast(ord_rfnd_ln_itms.order_id as string)=rfnd_adj.order_id and rfnd_adj.refund_date=ord_rfnd_ln_itms.refund_date
left join
(select cast(order_id as string) as order_id, date(created_at) as refund_date,
sum(coalesce(cast(refund_line_items_quantity as numeric),0)) return_quantity,
sum(coalesce(refund_line_items_subtotal,0)) refund_price, sum(coalesce(refund_line_items_total_tax,0)) refund_tax
from {{ref('ShopifyRefundsRefundLineItems')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1,2) rfnd_lns
on cast(ord_rfnd_ln_itms.order_id as string)=rfnd_lns.order_id and rfnd_lns.refund_date=ord_rfnd_ln_itms.refund_date
left join (select distinct email, customer_id, order_id, date(created_at) as order_date from  {{ ref('ShopifyOrdersCustomer') }}
-- where email is not null
qualify row_number() over(partition by order_id order by date(updated_at) desc)=1
) info
on ord_rfnd_ln_itms.order_id = info.order_id
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} exg on info.order_date = exg.date and rfnd_tran.transactions_currency = exg.to_currency_code
{% endif %}
-- fetching the subscription ids in case of recharge orders

 
-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel,
  cast(shopify_order_id as string) as order_id,
--   items_sku as sku,
--   subscription_id
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ref.order_id= upscribe.order_id and ref.line_item_sku = upscribe.sku
{% endif %}
 
where
ord_rfnd_ln_itms.refund_date is not null
group by 1,2,3,4,5,6,7,8,10,11,12