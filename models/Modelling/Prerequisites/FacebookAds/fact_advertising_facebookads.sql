with parent as (
    select 
    brand,
    store,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name, 
    ad_id,
    date(date_start) as date,
    exchange_currency_rate,
    exchange_currency_code,
    sum(clicks) clicks,
    sum(cast(impressions as numeric)) as impressions,
    cast(null as numeric) quantity,
    sum(spend) spend
    from {{ref('FacebookAdinsights')}}
    group by 1,2,3,4,5,6,7,8,9,10
    ),

child as (
    select 
    brand,
    store,
    campaign_id,
    campaign_name,
    adset_id, 
    adset_name, 
    ad_id,
    date(date_start) as date,
    count(cast(action_values_value as numeric)) as conversions,
    sum(cast(action_values_value as numeric)) as sales
    from {{ref('FacebookAdinsightsActionValues')}}
    where action_values_action_type = 'offsite_conversion.fb_pixel_purchase'
    group by 1,2,3,4,5,6,7,8
    )

select 
a.brand,
{{ store_name('a.store') }},
cast(a.campaign_id as string) as campaign_id,
cast(null as string) as flow_id,
cast(a.adset_id as string) as adgroup_id, 
cast(a.ad_id as string) as ad_id,
cast(null as string) as product_id,
cast(null as string) as sku,
cast(null as string) as variant_id,
a.date,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
'Facebook' as ad_channel,
'Facebook' as campaign_type,
'Facebook' as ad_type,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(conversions) as conversions,
sum(quantity) quantity,
sum(spend) as spend,
sum(sales) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from parent a left join child b
on a.brand  = b.brand and a.store = b.store and a.campaign_id = b.campaign_id and a.adset_id = b.adset_id and a.adset_name = b.adset_name and a.ad_id = b.ad_id and a.date = b.date 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
