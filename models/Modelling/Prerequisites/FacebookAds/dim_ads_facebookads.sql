select * {{exclude()}} (row_num) from 
    (
    select 
    ad_id,
    'Facebook' as ad_channel,
    ad_name,
    'Facebook' as ad_type,
    'Facebook' as campaign_type,
    CASE 
    WHEN LOWER(ad_name) like '%static%' then 'Static'
    WHEN LOWER(ad_name) like '%avs%' then 'Advertorial'
    WHEN LOWER(ad_name) like '%vid%' then 'Video'    
    END as creative_type,
    CASE 
    WHEN LOWER(campaign_name) like '%acq%' then 'Acquisition'
    WHEN LOWER(campaign_name) like '%awareness%' then 'Awareness'
    WHEN LOWER(campaign_name) like '%retarget%' then 'Retargeting'
    WHEN LOWER(campaign_name) like '%retention%' then 'Retention'
    WHEN LOWER(campaign_name) like '%creative test%' then 'Creative Test'
    
    END funnel_type,
    CASE WHEN LOWER(campaign_name) like '%international%' then 'International'
    ELSE 'US'
    END country_type,
    adset_id as adgroup_id,
    adset_name as adgroup_name,
    campaign_name as campaign_name,
    date(date_start) as last_updated_date,
    row_number() over(partition by ad_id order by date(date_start) desc) row_num
    from {{ ref('FacebookAdinsights') }}
    )
where row_num = 1