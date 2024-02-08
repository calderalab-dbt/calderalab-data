{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
brand,
{{ store_name('marketplaceName') }},
'Amazon Seller Central' as platform_name,
childASIN as product_id,
cast(null as string) as sku,
cast(null as string) as source,
cast(null as string) as medium,
cast(null as string) as campaign,
cast(null as string) as keyword,
cast(null as string) as content,
date,
exchange_currency_rate,
exchange_currency_code,
sum(mobileAppSessions) as mobile_sessions,
sum(browserSessions) as browser_sessions,
cast(null as int) tablet_sessions,
sum(sessions) as sessions,
sum(mobileAppPageViews) as mobile_pageviews,
sum(browserPageViews) as browser_pageviews,
cast(null as int) tablet_pageviews,
sum(pageViews) as pageviews,   
cast(null as int) mobile_clicks,
cast(null as int) browser_clicks,
cast(null as int) tablet_clicks,
cast(null as int) clicks,
cast(null as int) mobile_impressions,
cast(null as int) browser_impressions,
cast(null as int) tablet_impressions,
cast(null as int) impressions,
cast(null as int) mobile_conversions,
cast(null as int) browser_conversions,
cast(null as int) tablet_conversions,
cast(null as int) conversions,
cast(null as int) spend,
sum(cast(null as numeric)) as glance_views, 
avg(buyBoxPercentage) as buybox_percentage,
sum(unitsOrdered) as quantity,
sum(orderedProductSales_amount) as product_sales
from {{ ref('SalesAndTrafficReportByChildASIN') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(ReportstartDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ReportstartDate)") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13