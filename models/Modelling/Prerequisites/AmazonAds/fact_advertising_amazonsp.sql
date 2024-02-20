{% if var('AMAZONSPADS') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
brand,
{{ store_name('countryName') }},
campaignId as campaign_id,
cast(null as string) as flow_id,
adGroupId as adgroup_id, 
adId as ad_id,
asin as product_id,
sku,
date(reportDate) as date,
exchange_currency_rate,
exchange_currency_code,
'Amazon Seller Central' as platform_name,
'Amazon' as ad_channel,
'Sponsored Products' as campaign_type,
'Sponsored Products' as ad_type,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(attributedConversions7d) as conversions,
sum(attributedUnitsOrdered7d) as quantity,
sum(cost) as spend,
sum(attributedSales7d) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from {{ ref('SPProductAdsReport')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15