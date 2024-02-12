select 
brand,
{{store_name('store')}},
coalesce(cast(CampaignID as string),'N/A') as campaign_id,
cast(null as string) as flow_id,
cast(AdgroupID as string) as adgroup_id, 
cast(AdID as string) as ad_id, 
cast(null as string) as product_id,
cast(null as string) as sku,
cast(null as string) as variant_id,
cast(null as numeric) as unq_add_to_cart,
cast(null as numeric) as unq_checkout_ini,
cast(null as numeric) as cart_conv_rate,
cast(null as numeric) as ini_checkout_conv_rate,
date as date,
cast(1 as decimal) as exchange_currency_rate,
cast('USD' as string) as exchange_currency_code, 
-- exchange_currency_rate,
-- exchange_currency_code,
'TikTok' as platform_name,
'TikTok' as ad_channel,
'TikTok' as campaign_type,
'TikTok' as ad_type,
sum(cast(Clicks as int)) clicks,
sum(cast(Impression as int)) impressions,
sum(cast(Conversions as int)) conversions,
sum(cast(null as int)) quantity,
sum(cast(Cost as numeric)) as spend,
sum(cast(TotalCompletePaymentValue as numeric)) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from 
(
select *,
from {{ ref('TiktokAds_ad_report_daily') }} ) a
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
