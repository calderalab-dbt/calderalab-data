select 
brand,
{{store_name('store')}},
cast(campaign_name as string) campaign_id,
cast(null as string) as flow_id,
cast(null as string) as adgroup_id, 
cast(null as string) as ad_id,
cast(null as string) as product_id,
cast(null as string) as sku,
cast(null as string) as variant_id,
cast(null as numeric) as unq_add_to_cart,
cast(null as numeric) as unq_checkout_ini,
cast(null as numeric) as cart_conv_rate,
cast(null as numeric) as ini_checkout_conv_rate,
date,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
'Google' as ad_channel,
'' as campaign_type,
'' as ad_type,
sum(cast(clicks as numeric)) clicks,
sum(cast(impressions as numeric)) impressions,
sum(conversions) conversions,
sum(cast(null as numeric)) as quantity,
sum(round((cast(cost_micros as numeric)/1000000),2)) as spend,
sum(conversions_value) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from {{ ref('GoogleAdsCampaign') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19