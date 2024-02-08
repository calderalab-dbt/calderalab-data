{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
brand,
'Amazon Vendor Central' as platform_name,
'Sourcing' as distribution_view,
{{ store_name('marketplaceName') }},
asin as product_id, 
cast(null as string) as sku,
shippedrevenue_currencycode as currency,
exchange_currency_code,
exchange_currency_rate,
date(startdate) as order_date,
sum(shippedUnits) shipped_units,
sum(shippedcogs_amount) shippedcogs,
sum(shippedRevenue_amount) shipped_revenue,
sum(cast(null as numeric)) as ordered_revenue,
sum(customerReturns) customer_returns,
cast(null as numeric) as orderedunits, ----solutions team added this
from {{ ref('VendorSalesReportBySourcing') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10

union all

select
brand,
'Amazon Vendor Central' as platform_name,
'Manufacturing' as distribution_view,
{{ store_name('marketplaceName') }},
asin as product_id, 
cast(null as string) as sku,
orderedrevenue_currencycode as currency,
exchange_currency_code,
exchange_currency_rate,
startdate as order_date,
sum(shippedUnits) quantity,
sum(shippedcogs_amount) shippedcogs,
sum(shippedRevenue_amount) shipped_revenue,
sum(orderedRevenue_amount) as ordered_revenue,
sum(customerReturns) customer_returns ,
sum(orderedunits) orderedunits    ------ solutions team has added this.
from {{ ref('VendorSalesReportByManufacturing') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10