{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
brand,
date,
amount_type,
transaction_type,
coalesce(charge_type,'') charge_type,
amazonorderid as order_id,
store_name,
asin1 as product_id,
sellerSKU as sku,
exchange_currency_code,
platform_name,
amount
from (
    select 
    brand,
    date(ShipmentEventlist_PostedDate) as date,
    case 
    when ItemFeeList_FeeType in ('GiftwrapChargeback','FBAWeightBasedFee','VariableClosingFee','ShippingChargeback','FBAPerUnitFulfillmentFee','FBAPerOrderFulfillmentFee') then 'Fulfilment and Shipping'  
    when ItemFeeList_FeeType in ('SalesTaxCollectionFee','Commission') then 'Selling Fees'  
    else 'Other Expenses' end as amount_type,
    'Order' as transaction_type,
    ItemFeeList_FeeType as charge_type,
    ShipmentEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('ShipmentEventlist_MarketplaceName') }},
    ShipmentItemlist_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((FeeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsOrderFees')}}
    where FeeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(ShipmentEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ShipmentEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(ShipmentEventlist_PostedDate) as date,
    'Promotions' as amount_type,
    'Order' as transaction_type,
    PromotionList_PromotionType as charge_type,
    ShipmentEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('ShipmentEventlist_MarketplaceName') }},
    ShipmentItemlist_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((PromotionAmount_CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsOrderPromotions')}}
    where PromotionAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(ShipmentEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ShipmentEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(ShipmentEventlist_PostedDate) as date,
    case 
    when ItemChargeList_ChargeType in ('Principal') then 'Product Sales' 
    when ItemChargeList_ChargeType in ('Tax') then 'Product Sales Tax' 
    else 'Other Income' end as amount_type,
    'Order' as transaction_type,
    ItemChargeList_ChargeType as charge_type,
    ShipmentEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('ShipmentEventlist_MarketplaceName') }},
    ShipmentItemlist_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((ChargeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsOrderRevenue')}}
    where ChargeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(ShipmentEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ShipmentEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(ShipmentEventlist_PostedDate) as date,
    'Other Income' as amount_type,
    'Order' as transaction_type,
    TaxesWithheld_ChargeType as charge_type,
    ShipmentEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('ShipmentEventlist_MarketplaceName') }},
    ShipmentItemlist_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((ChargeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsOrderTaxes')}}
    where ChargeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(ShipmentEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ShipmentEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(RefundEventlist_PostedDate) as date,
    case 
    when ItemFeeAdjustmentList_FeeType in ('Commission','RefundCommission') then 'Selling Fees'  
    when ItemFeeAdjustmentList_FeeType in ('GiftwrapChargeback') then 'Fulfilment and Shipping'  
    else 'Other Income' end as amount_type,
    'Refund' as transaction_type,
    ItemFeeAdjustmentList_FeeType as charge_type,
    RefundEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('RefundEventlist_MarketplaceName') }},
    ShipmentItemAdjustmentList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((FeeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsRefundFees')}}
    where RefundEventlist_PostedDate is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(RefundEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RefundEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(RefundEventlist_PostedDate) as date,
    'Promotions' as amount_type,
    'Refund' as transactionType,
    PromotionList_PromotionType as charge_type,
    RefundEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('RefundEventlist_MarketplaceName') }},
    ShipmentItemAdjustmentList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((PromotionAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsRefundPromotions')}}
    where PromotionAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(RefundEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RefundEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(RefundEventlist_PostedDate) as date,
    case 
    when ItemChargeAdjustmentList_ChargeType in ('Tax','Principal') then 'Refunded Sales'  
    else 'Refunds and Returns' end as amount_type,
    'Refund' as transaction_type,
    ItemChargeAdjustmentList_ChargeType as charge_type,
    RefundEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('RefundEventlist_MarketplaceName') }},
    ShipmentItemAdjustmentList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((ChargeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsRefundRevenue')}}
    where ChargeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(RefundEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RefundEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(RefundEventlist_PostedDate) as date,
    'Refunds and Returns' as amount_type,
    'Refund' as transaction_type,
    TaxesWithheld_ChargeType as charge_type,
    RefundEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('RefundEventlist_MarketplaceName') }},
    ShipmentItemAdjustmentList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((ChargeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsRefundTaxes')}}
    where ChargeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(RefundEventlist_PostedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RefundEventlist_PostedDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(RequestStartDate) as date,
    case
    when FeeList_FeeType in ('FBADisposalFee','FBADisposalFee','FBALongTermStorageFee','FBACustomerReturnPerUnitFee','FBAPerUnitFulfillmentFee','FBARemovalFee','FBAInboundTransportationFee','FBAWeightBasedFee','FBAStorageFee') then 'Fulfilment and Shipping'  
    else 'Other Expenses' end as amount_type,
    'Service' as transaction_type,
    FeeList_FeeType as charge_type,
    ServiceFeeEventList_AmazonOrderId as amazonorderid,
    {{ store_name('marketplaceName') }},
    ServiceFeeEventList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((FeeAmount_CurrencyAmount/exchange_currency_rate),2)) as amount
    from {{ ref('ListFinancialEventsServiceFees')}}
    where FeeAmount_CurrencyAmount is not null
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        and date(RequestStartDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RequestStartDate)") }}
    {% endif %}
    and exchange_currency_code is not null
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(RequestStartDate) as date,
    'Reimbursements' as amount_type,
    'Adjustments' as transaction_type,
    'Reimbursement' as charge_type,
    ServiceFeeEventList_AmazonOrderId as amazonorderid,
    {{ store_name('marketplaceName') }},
    ServiceFeeEventList_SellerSKU as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(0) amount
    from {{ ref('ListFinancialEventsServiceFees')}}
    WHERE
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        date(RequestStartDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(RequestStartDate)") }} and
    {% endif %}
    exchange_currency_code is not null
    group by 1,2,3,4,5,6,7,8,9,10

{% if var('product_details_gs_flag') %}
    UNION ALL 

    select
    brand,
    date(purchase_date) as date,
    'Cost of Goods Sold' as amount_type,
    'Input Costs' as transaction_type,
    'Cost of Goods' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('marketplacename') }},
    a.sku as sellerSKU,
    currency as exchange_currency_code,
    platform_name,
    sum(0-(ifnull(cast(cogs as decimal),0)*ifnull(quantity,0))) amount
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} a
    left join {{ ref('ProductDetailsInsights')}} b
    on lower(a.sku) = lower(b.sku) 
    where currency is not null and date(a.purchase_date) BETWEEN b.start_date AND b.end_date
    group by 1,2,3,4,5,6,7,8,9,10
{% endif %}

{% if var('AMAZONSPADS') %}
    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising' as amount_type,
    'Advertising' as transaction_type,
    'Sponsored Products' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    sku as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from {{ ref('SPProductAdsReport')}}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(reportDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(reportDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10
{% endif %}
{% if var('AMAZONSDADS') %}
    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising' as amount_type,
    'Advertising' as transaction_type,
    'Sponsored Display' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    sku as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from {{ ref('SDProductAdsReport')}}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(reportDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(reportDate)") }}
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10
{% endif %}
{% if var('AMAZONSBADS') %}
    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising' as amount_type,
    'Advertising' as transaction_type,
    'Sponsored Brands' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    cast(null as string) as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from (
        select *,
        {% if var('currency_conversion_flag') %}
        case when b.value is null then 1 else b.value end as exchange_currency_rate,
        case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
        {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        cast(a.currency as string) as exchange_currency_code, 
        {% endif %}
        from (
            select *,
            {{currency_code('countryName')}} 
            from {{ ref('SBUnifiedAdGroupsReport')}} 
            {% if not flags.FULL_REFRESH %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE date(reportDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(reportDate)") }}
            {% endif %}
            ) a 
        {% if var('currency_conversion_flag') %}
        left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
        {% endif %}
        ) 
    group by 1,2,3,4,5,6,7,8,9,10
{% endif %}
    UNION ALL 

    select
    brand,
    date(ShipmentEventlist_PostedDate) as date,
    'Units' as amount_type,
    'Order' as transaction_type,
    'Units' as charge_type,
    ShipmentEventlist_AmazonOrderId as amazonorderid,
    {{ store_name('ShipmentEventlist_MarketplaceName') }},
    ShipmentItemList_SellerSKU as sellerSKU,
    'NA' as exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(ShipmentItemList_QuantityShipped) as amount
    from {{ ref('ListFinancialEventsOrderRevenue')}}
    where ItemChargeList_ChargeType in ('Principal')
    group by 1,2,3,4,5,6,7,8,9,10

  


) lfe
left join (select distinct seller_sku, asin1 from {{ ref('AllListingsReport')}}) listings
on lfe.sellerSKU = listings.seller_sku 