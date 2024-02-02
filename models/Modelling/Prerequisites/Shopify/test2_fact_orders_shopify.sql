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
cast(customer_id as string)customer_id,
sum(order_quantity) quantity,
sum(cast(order_price AS numeric) + ifnull(CAST(total_tax AS numeric),0)) total_price,
ifnull(sum(cast(order_price AS numeric)),0) subtotal_price,
ifnull(sum(cast(total_tax AS numeric)),0) total_tax,
ifnull(sum(cast(order_shipping_price as numeric)),0) as shipping_price,
cast(null as numeric) as giftwrap_price,
ifnull( sum(cast(total_discounts AS numeric)),0) as order_discount,
ifnull( sum(cast(shipping_discount as numeric)),0) as shipping_discount
-- from
-- ( select *
from {{ ref('ShopifyOrders') }} ord
left join (select distinct customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }} where customer_id is not null) info
on cast(ord.order_id as string) = cast(info.order_id as string)
inner join
    (select order_id,brand, store, sum(cast(line_items_quantity as numeric)) order_quantity, sum(CAST(line_items_price AS numeric)*line_items_quantity) order_price from {{ref('ShopifyOrdersLineItems')}} where not ifnull(line_items_gift_card,false)
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
    {% endif %}
    group by 1,2,3
    ) ord_ln_itms
on ord.order_id=ord_ln_itms.order_id and ord.brand = ord_ln_itms.brand and ord.store = ord_ln_itms.store
left join
    (select order_id,brand,sum(cast(shipping_lines_price as numeric)) order_shipping_price,sum(cast(case when discount_type = 'shipping' then discount_amount end  as numeric))  as shipping_discount from {{ref('ShopifyOrdersShippingLines')}}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
    {% endif %}
    group by 1,2
    ) ord_shp_lns
on ord.order_id=ord_shp_lns.order_id and ord.brand = ord_shp_lns.brand
group by 1,2,3,4,5,6,7,8,9,10,11
 
 
 
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
cast(customer_id as string) as customer_id,
sum(cast(return_quantity as numeric)) quantity,
sum(cast(transactions_amount as numeric)) total_price,
sum(case when lower(transactions_status) = 'success' then cast(ifnull(transactions_amount,0)-ifnull(refund_tax,0)-ifnull(gift_amount,0) as numeric) end ) as subtotal_price,
sum(cast(refund_tax as numeric)) total_tax,
sum(cast(adjustments_amount as numeric)) as shipping_price,
sum(cast(gift_amount as numeric)) as giftwrap_price,
sum(cast(refund_discount as numeric)) order_discount,
sum(cast(null as numeric)) as shipping_discount
from
(select brand, store, transactions_currency, exchange_currency_code, exchange_currency_rate,transactions_status, order_id, cast(refund_id as string) refund_id, date(created_at) refund_date, sum(cast(transactions_amount as numeric)) transactions_amount from {{ ref('ShopifyRefundsTransactions')}}
where
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }} and
{% endif %}
transactions_amount is not null group by 1,2,3,4,5,6,7,8,9) rfnd_tran
left join (select distinct customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }} where customer_id is not null) info
on cast(rfnd_tran.order_id as string)=cast(info.order_id as string)
   
left join (select cast(refund_id as string) as refund_id, sum(cast(refund_line_items_quantity as numeric)) return_quantity, sum(refund_line_items_subtotal) refund_price, sum(refund_line_items_total_tax) refund_tax from {{ref('ShopifyRefundsRefundLineItems')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1) rfnd_lns
on cast(rfnd_tran.refund_id as string)=rfnd_lns.refund_id
    left join
            (select cast(refund_id as string) as refund_id, sum(cast(line_item_total_discount as numeric)) refund_discount, sum(case when line_item_gift_card then  
    refund_line_items_subtotal end )gift_amount from {{ref('ShopifyRefundsLineItems')}}
            {% if not flags.FULL_REFRESH %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
            {% endif %}
            group by 1
            ) ord_rfnd_ln_itms
    on cast(rfnd_tran.refund_id as string)=ord_rfnd_ln_itms.refund_id
 
left join
(select amount_set_presentment_money_currency_code, order_id,
date(created_at) as refnd_date,
sum(coalesce(cast(amount_set_presentment_money_amount as numeric),0))*-1 adjustments_amount
from {{ ref('ShopifyRefundsOrderAdjustments')}}
where order_adjustments_kind!='refund_discrepancy'
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    and date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1,2,3) rfnd_adj
on cast(rfnd_tran.order_id as string)=rfnd_adj.order_id and rfnd_adj.refnd_date=rfnd_tran.refund_date
 
 
where
refund_date is not null
group by 1,2,3,4,5,6,7,8,9,10,11