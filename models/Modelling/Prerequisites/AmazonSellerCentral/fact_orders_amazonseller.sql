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
    currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(purchase_date) as date,
    'Order' as transaction_type,
    coalesce(lst_ord.BuyerInfo_BuyerEmail,'') as customer_id,
    case when lower(order_status) = 'cancelled' then true else false end as is_cancelled,
    is_business_order,
    max(case when instr(lower(promotion_ids),'subscribe')>0 then True else False end) as is_subscription_order,
    sum(quantity) quantity,
    sum(ifnull(item_price,0) + ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_price,
    sum(item_price) subtotal_price,
    sum(ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_tax, 
    sum(shipping_price) shipping_price, 
    sum(gift_wrap_price) giftwrap_price,
    sum(item_promotion_discount) order_discount,
    sum(ship_promotion_discount) shipping_discount
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
    left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
    on ord.amazon_order_id = lst_ord.amazonorderid
    WHERE
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        date(last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(last_updated_date)") }} and
    {% endif %}
    sales_channel <> 'Non-Amazon' and item_status != 'Cancelled'
    group by 1,2,3,4,5,6,7,8,9,10,11,12
    
union all

    select 
    fba_rtrn.order_id,
    brand,
    'Amazon Seller Central' as platform_name,
    {{ store_name('marketplaceName') }},
    ord.currency,
    ord.exchange_currency_code,
    ord.exchange_currency_rate,
    date(return_date) as date,
    'Return' as transaction_type,  
    lst_ord.BuyerInfo_BuyerEmail as customer_id,
    false as is_cancelled,
    is_business_order,
    is_subscription_order,
    coalesce(sum(fba_rtrn.quantity),cast(null as numeric)) quantity,
    coalesce(sum((ifnull(item_price,0) + ifnull(item_tax,0))/nullif(ord.quantity,0)*fba_rtrn.quantity),cast(null as numeric)) total_price,
    cast(null as numeric) as subtotal_price,
    cast(null as numeric) as total_tax,
    cast(null as numeric) as shipping_price,
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as order_discount,
    cast(null as numeric) as shipping_discount
    from {{ ref('FBAReturnsReport') }} fba_rtrn
    left join (
        select amazon_order_id, currency, exchange_currency_rate, exchange_currency_code, 
        is_business_order,
        max(case when instr(lower(promotion_ids),'subscribe')>0 then True else False end) as is_subscription_order,
        sum(item_price) as item_price, sum(item_tax) item_tax, sum(quantity) as quantity
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where 
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            date(last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-180, from_date_or_timestamp="date(last_updated_date)") }} and
        {% endif %}
        item_status != 'Cancelled'
        group by 1,2,3,4,5) ord
    on fba_rtrn.order_id = ord.amazon_order_id
    left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
    on fba_rtrn.order_id = lst_ord.amazonorderid
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(fba_rtrn.ReportRequestTime) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(fba_rtrn.ReportRequestTime)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    
    UNION ALL

    select
    order_id,
    brand,
    'Amazon Seller Central' as platform_name,
    {{ store_name('marketplaceName') }},
    Currency_code as currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(Return_request_date) as date,
    'Return' as transaction_type,  
    lst_ord.BuyerInfo_BuyerEmail as customer_id,
    false as is_cancelled,
    is_business_order,
    is_subscription_order,
    coalesce(sum(Return_quantity),cast(null as numeric)) quantity,
    coalesce(sum(Refunded_Amount),cast(null as numeric)) total_price,
    cast(null as numeric) as subtotal_price,
    cast(null as numeric) as total_tax,
    cast(null as numeric) as shipping_price,
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as order_discount,
    cast(null as numeric) as shipping_discount
    from {{ref('FlatFileReturnsReportByReturnDate')}} ffrr
    left join (
        select amazon_order_id,
        is_business_order,
        max(case when instr(lower(promotion_ids),'subscribe')>0 then True else False end) as is_subscription_order
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where 
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            date(last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-180, from_date_or_timestamp="date(last_updated_date)") }} and
        {% endif %}
        item_status != 'Cancelled'
        group by 1,2
        ) ord
    on ffrr.order_id = ord.amazon_order_id
    left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
    on ffrr.order_id = lst_ord.amazonorderid
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where date(ReportRequestTime) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ReportRequestTime)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
