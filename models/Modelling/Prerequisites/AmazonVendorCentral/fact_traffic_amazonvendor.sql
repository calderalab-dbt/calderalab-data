{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
brand,
{{ store_name('marketplaceName') }},
'Amazon Vendor Central' as platform_name,
asin as product_id,
cast(null as string) as sku,
cast(null as string) as source,
cast(null as string) as medium,
cast(null as string) as campaign,
cast(null as string) as keyword,
cast(null as string) as content,
date(startDate) as date,
1 as exchange_currency_rate,
cast(null as string) as exchange_currency_code,
sum(cast(null as int)) mobile_sessions,
sum(cast(null as int)) browser_sessions,
sum(cast(null as int)) tablet_sessions,
sum(cast(null as int)) as sessions,
sum(cast(null as int)) mobile_pageviews,
sum(cast(null as int)) browser_pageviews,
sum(cast(null as int)) tablet_pageviews,
sum(cast(null as int)) as pageviews,
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
sum(cast(glanceViews as int)) as glance_views,
avg(cast(null as numeric)) as buybox_percentage,
sum(cast(null as int)) as quantity,
sum(cast(null as numeric)) as product_sales
from {{ ref('VendorTrafficReport') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

