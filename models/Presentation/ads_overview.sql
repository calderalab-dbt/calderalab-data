{{ config(
    materialized='table'
)}}

SELECT
  date,
  sum(case when ad_channel = 'Facebook' then ifnull(spend,0) end) as fb_spend,
  sum(case when ad_channel = 'YouTube' then ifnull(spend,0) end) as yt_spend,
  sum(case when ad_channel = 'TikTok' then ifnull(spend,0) end) as tiktok_spend,
  sum(case when ad_channel = 'Google' then ifnull(spend,0) end) as google_spend,
  sum(case when ad_channel = 'Google' and LOWER(campaign_key) LIKE '%brand%' AND LOWER(campaign_key) LIKE '%search%' then ifnull(spend,0) end) as google_brand_spend,
  sum(case when ad_channel = 'Facebook' then ifnull(conversions,0) end) as fb_conversions,
  sum(case when ad_channel = 'YouTube' then ifnull(conversions,0) end) as yt_conversions,
  sum(case when ad_channel = 'TikTok' then ifnull(conversions,0) end) as tiktok_conversions,
  sum(case when ad_channel = 'Google' then ifnull(conversions,0) end) as google_conversions,
  sum(case when ad_channel = 'Google' and LOWER(campaign_key) LIKE '%brand%' AND LOWER(campaign_key) LIKE '%search%' then ifnull(conversions,0) end) as google_brand_conversions,
  sum(case when ad_channel = 'Google' and LOWER(campaign_key) LIKE '%non%brand%' AND LOWER(campaign_key) LIKE '%search%' then ifnull(conversions,0) end) as google_non_brand_conversions,
  sum(case when ad_channel = 'Facebook' then ifnull(sales,0) end) as fb_sales,
  sum(case when ad_channel = 'YouTube' then ifnull(sales,0) end) as yt_sales,
  sum(case when ad_channel = 'TikTok' then ifnull(sales,0) end) as tiktok_sales,
  sum(case when ad_channel = 'Google' then ifnull(sales,0) end) as google_sales,
  sum(case when ad_channel = 'Google' and LOWER(campaign_key) LIKE '%brand%' AND LOWER(campaign_key) LIKE '%search%' then ifnull(sales,0) end) as google_brand_sales,
  sum(case when ad_channel = 'Google' and LOWER(campaign_key) LIKE '%non%brand%' AND LOWER(campaign_key) LIKE '%search%' then ifnull(sales,0) end) as google_non_brand_sales,
FROM
  {{ref('fact_advertising')}}
group by 1
order by 1 desc
LIMIT
  1000