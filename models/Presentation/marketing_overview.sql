Select 
brand_name,
portfolio_name,
d.ad_channel,
d.campaign_name,
d.campaign_type,
d.status as campaign_status,
d.budget as campaign_budget,
d.budget_type as campaign_budget_type,
c.platform_name,
store_name,
date,
sum(spend) adspend,
sum(sales) adsales,
sum(clicks) clicks,
sum(impressions) impressions,
sum(conversions) conversions,
sum(email_deliveries) email_deliveries,
sum(email_opens) email_opens,
sum(email_unsubscriptions) email_unsubscriptions
from {{ ref('fact_advertising')}} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join {{ ref('dim_campaign')}} d
on a.campaign_key = d.campaign_key
group by 1,2,3,4,5,6,7,8,9,10,11

