{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
  cast(null as string) as order_id,
  cast(null as string) as subscription_id,
  brand,
  'Amazon Vendor Central' as platform_name,
  {{ store_name('marketplaceName') }},
  shippedrevenue_currencycode as currency,
  exchange_currency_code,
  exchange_currency_rate,
  date(startDate) as date,
  'Order' as transaction_type, 
  false as is_cancelled,
  cast(null as boolean) as is_business_order,
  cast(null as boolean) as is_subscription_order,
  coalesce(sum(shippedUnits),cast(null as numeric)) quantity,
  coalesce(sum(shippedcogs_amount),cast(null as numeric)) total_price,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  cast(null as numeric) as order_discount,
  cast(null as numeric) as shipping_discount,
  cast(null as string) as customer_id
  from {{ ref('VendorSalesReportBySourcing') }} 
  {% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
  {% endif %}
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,19

  UNION ALL
  
  select
  cast(null as string) as order_id,
  cast(null as string) as subscription_id,
  brand,
  'Amazon Vendor Central' as platform_name,
  {{ store_name('marketplaceName') }},
  shippedrevenue_currencycode as currency,
  exchange_currency_code,
  exchange_currency_rate,
  date(startDate) as date,
  'Return' as transaction_type, 
  false as is_cancelled,
  cast(null as boolean) as is_business_order,
  cast(null as boolean) as is_subscription_order,
  coalesce(sum(customerReturns),cast(null as numeric)) quantity,
  coalesce(sum(shippedrevenue_amount),cast(null as numeric)) total_price,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  cast(null as numeric) as order_discount,
  cast(null as numeric) as shipping_discount,
  cast(null as string) as customer_id
  from {{ ref('VendorSalesReportBySourcing') }} 
  {% if not flags.FULL_REFRESH %}
      {# /* -- this filter will only be applied on an incremental run */ #}
      WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
  {% endif %}
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,19