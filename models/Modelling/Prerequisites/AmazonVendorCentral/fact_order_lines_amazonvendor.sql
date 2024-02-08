{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
cast(null as string) as order_id,
brand,
'Amazon Vendor Central' as platform_name,
{{ store_name('marketplaceName') }},
asin as product_id, 
cast(null as string) as sku,
shippedrevenue_currencycode as currency,
exchange_currency_code,
exchange_currency_rate,
date(startDate) as date,
cast(null as string) as subscription_id,
'Order' as transaction_type, 
false as is_cancelled,
cast(null as boolean) as is_business_order,
cast(null as boolean) as is_subscription_order,
'' as reason,
cast(null as string) as customer_id,
cast(null as string) as ship_address_type,
cast(null as string) as ship_address_1,
cast(null as string) as ship_address_2,
cast(null as string) as ship_city,
cast(null as string) as ship_district,
cast(null as string) as ship_state,
cast(null as string) as ship_country,
cast(null as string) as ship_postal_code,
cast(null as string) as bill_address_type,
cast(null as string) as bill_address_1,
cast(null as string) as bill_address_2,
cast(null as string) as bill_city,
cast(null as string) bill_district,
cast(null as string) bill_state,
cast(null as string) bill_country,
cast(null as string) bill_postal_code,
sum(shippedUnits) quantity,
sum(shippedcogs_amount) total_price,
cast(null as numeric) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('VendorSalesReportBySourcing') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33

UNION ALL

select
cast(null as string) as order_id,
brand,
'Amazon Vendor Central' as platform_name,
{{ store_name('marketplaceName') }},
asin as product_id, 
cast(null as string) as sku,
shippedrevenue_currencycode as currency,
exchange_currency_code,
exchange_currency_rate,
date(startDate) as date,
cast(null as string) as subscription_id,
'Return' as transaction_type, 
false as is_cancelled,
cast(null as boolean) as is_business_order,
cast(null as boolean) as is_subscription_order,
'' as reason,
cast(null as string) as customer_id,
cast(null as string) as ship_address_type,
cast(null as string) as ship_address_1,
cast(null as string) as ship_address_2,
cast(null as string) as ship_city,
cast(null as string) as ship_district,
cast(null as string) as ship_state,
cast(null as string) as ship_country,
cast(null as string) as ship_postal_code,
cast(null as string) as bill_address_type,
cast(null as string) as bill_address_1,
cast(null as string) as bill_address_2,
cast(null as string) as bill_city,
cast(null as string) bill_district,
cast(null as string) bill_state,
cast(null as string) bill_country,
cast(null as string) bill_postal_code,
sum(customerReturns) quantity,
sum(shippedrevenue_amount) total_price,
cast(null as numeric) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('VendorSalesReportBySourcing') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
    