{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
amazon_order_id as order_id,
brand,
'Amazon Seller Central' as platform_name,
{{ store_name('sales_channel') }},
asin as product_id, 
sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(purchase_date) as date,
case when instr(lower(promotion_ids),'subscribe')>0 then amazon_order_id else cast(null as string) end as subscription_id,
coalesce(lst_ord.BuyerInfo_BuyerEmail,'') as customer_id,
'Order' as transaction_type,
case when lower(order_status) = 'cancelled' or lower(item_status) = 'cancelled' then true else false end as is_cancelled,
is_business_order,
case when instr(lower(promotion_ids),'subscribe')>0 then True else False end as is_subscription_order,
cast(null as string) as reason,
sum(quantity) quantity,
sum(ifnull(item_price,0) + ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_price,
sum(item_price) subtotal_price,
sum(ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_tax, 
sum(shipping_price) shipping_price, 
sum(gift_wrap_price) giftwrap_price,
sum(item_promotion_discount) item_discount,
sum(ship_promotion_discount) shipping_discount 
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
    left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
    on ord.amazon_order_id = lst_ord.amazonorderid
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(ord.last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ord.last_updated_date)") }}
    {% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

union all

select 
order_id,
brand,
'Amazon Seller Central' as platform_name,
store_name,
product_id, 
sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date,
subscription_id,
lst_ord.BuyerInfo_BuyerEmail as customer_id,
'Return' as transaction_type,  
is_cancelled,
is_business_order,
is_subscription_order,
return_reason as reason,
quantity,
total_price,
subtotal_price, 
total_tax, 
shipping_price, 
giftwrap_price,
item_discount,
shipping_discount  
from (
    select 
    ret.order_id,
    brand,
    {{ store_name('marketplaceName') }},
    ret.asin as product_id, 
    ret.sku,
    ord.currency,
    ord.exchange_currency_code,
    ord.exchange_currency_rate,
    date(return_date) as date,
    reason as return_reason,
    subscription_id, 
    is_business_order,
    is_subscription_order,
    false as is_cancelled,
    sum(ret.quantity) as quantity,
    sum(((ifnull(item_price,0) + ifnull(item_tax,0))/nullif(ord.quantity,0)) * ret.quantity) as total_price,
    cast(null as numeric) as subtotal_price, 
    cast(null as numeric) as total_tax, 
    cast(null as numeric) as shipping_price, 
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as item_discount,
    cast(null as numeric) as shipping_discount 
    from {{ ref('FBAReturnsReport') }} ret
    left join (
        select ord.amazon_order_id, sku, currency, exchange_currency_rate, exchange_currency_code, 

        case when instr(lower(promotion_ids),'subscribe')>0 then amazon_order_id else cast(null as string) end as subscription_id,
        is_business_order,
        case when instr(lower(promotion_ids),'subscribe')>0 then True else False end as is_subscription_order,
        sum(item_price) as item_price, sum(item_tax) as item_tax, sum(quantity) as quantity
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
        where item_status != 'Cancelled'
        group by 1,2,3,4,5,6,7,8) ord
    on ret.order_id = ord.amazon_order_id and ret.sku = ord.sku
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(ret.ReportstartDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ret.ReportstartDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    
    UNION ALL

    select
    order_id,
    brand,
    {{ store_name('marketplaceName') }},
    asin as product_id, 
    Merchant_SKU as sku,
    Currency_code as currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(Return_request_date) as date,
    Return_Reason as return_reason,
    subscription_id, 
    is_business_order,
    is_subscription_order,
    false as is_cancelled,
    sum(Return_quantity) as quantity,
    sum(Refunded_Amount) as total_price,
    cast(null as numeric) as subtotal_price, 
    cast(null as numeric) as total_tax, 
    cast(null as numeric) as shipping_price, 
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as item_discount,
    cast(null as numeric) as shipping_discount 
    from {{ref('FlatFileReturnsReportByReturnDate')}} a
    left join (
        select distinct amazon_order_id,
        case when instr(lower(promotion_ids),'subscribe')>0 then amazon_order_id else cast(null as string) end as subscription_id,
        is_business_order,
        case when instr(lower(promotion_ids),'subscribe')>0 then True else False end as is_subscription_order
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where 
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            date(last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-180, from_date_or_timestamp="date(last_updated_date)") }} and
        {% endif %}
        item_status != 'Cancelled'
        ) ord
    on a.order_id = ord.amazon_order_id
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(a.ReportstartDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(a.ReportstartDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) rtrn
left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
on rtrn.order_id = lst_ord.amazonorderid