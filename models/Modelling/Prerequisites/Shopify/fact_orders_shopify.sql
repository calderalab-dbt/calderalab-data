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
false as is_cancelled,
sum(order_quantity) quantity,
sum(cast(order_price AS numeric) + ifnull(CAST(total_tax AS numeric),0)) total_price,
sum(cast(order_price AS numeric)) subtotal_price,
sum(cast(total_tax AS numeric)) total_tax, 
sum(cast(order_shipping_price as numeric)) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(cast(total_discounts AS numeric)) as order_discount,
sum(cast(null as numeric)) as shipping_discount
-- customer_id
-- from 
-- ( select *
from {{ ref('ShopifyOrders') }} ord
-- left join (select distinct customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }} where customer_id is not null) info
-- on ord.order_id = info.order_id
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
group by 1,2,3,4,5,6,7,8,9,10

UNION ALL

select 
cast(rfnd_tran.order_id as string) as order_id,
brand,
'Shopify' as platform_name,
{{store_name('store')}},
rfnd_tran.transactions_currency as currency,
rfnd_tran.exchange_currency_code,
rfnd_tran.exchange_currency_rate,
date(rfnd_tran.refund_date) as date,
'Return' as transaction_type, 
false as is_cancelled,
sum(cast(return_quantity as numeric)) quantity,
sum(cast(transactions_amount as numeric)) total_price,
sum(cast(refund_price as numeric)) as subtotal_price, 
sum(cast(refund_tax as numeric)) total_tax,
cast(null as numeric) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(cast(refund_discount as numeric)) order_discount,
cast(null as numeric) as shipping_discount
-- cast(null as string) as customer_id
from 
(select brand, store, transactions_currency, exchange_currency_code, exchange_currency_rate, order_id, cast(refund_id as string) refund_id, date(created_at) refund_date, sum(cast(transactions_amount as numeric)) transactions_amount from {{ ref('ShopifyRefundsTransactions')}} 
where 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }} and
{% endif %}
transactions_amount is not null group by 1,2,3,4,5,6,7,8) rfnd_tran
left join (select cast(refund_id as string) as refund_id, sum(cast(refund_line_items_quantity as numeric)) return_quantity, sum(refund_line_items_subtotal) refund_price, sum(refund_line_items_total_tax) refund_tax from {{ref('ShopifyRefundsRefundLineItems')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1) rfnd_lns
on cast(rfnd_tran.refund_id as string)=rfnd_lns.refund_id
left join (select cast(refund_id as string) as refund_id, sum(cast(line_item_total_discount as numeric)) refund_discount from {{ref('ShopifyRefundsLineItems')}} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1) ord_rfnd_ln_itms
on cast(rfnd_tran.refund_id as string)=ord_rfnd_ln_itms.refund_id
where 
refund_date is not null
group by 1,2,3,4,5,6,7,8,9,10
