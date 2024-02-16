Select 
brand_name,
portfolio_name,
g.ad_channel,
e.campaign_type,
c.platform_name,
store_name,
date,
campaign_name,
e.status as campaign_status,
e.budget as campaign_budget,
e.budget_type as campaign_budget_type,
coalesce(campaign_placement,'') campaign_placement,
coalesce(product_id,'') product_id,
coalesce(product_name,'') product_name,
coalesce(sku,'') sku,
sum(spend) adspend,
sum(sales) adsales,
sum(clicks) clicks,
sum(impressions) impressions,
sum(conversions) conversions,
sum(email_deliveries) email_deliveries,
sum(email_opens) email_opens,
sum(email_unsubscriptions) email_unsubscriptions,
from {{ ref('fact_advertising')}} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}}) d 
on a.product_key = d.product_key 
left join {{ ref('dim_campaign')}} e
on a.campaign_key = e.campaign_key
left join {{ ref('dim_adgroup')}} f
on a.adgroup_key = f.adgroup_key
left join {{ ref('dim_ads')}} g
on a.ad_key = g.ad_key
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15