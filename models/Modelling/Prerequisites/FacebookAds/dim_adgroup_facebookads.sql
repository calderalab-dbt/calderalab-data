select * {{exclude()}} (row_num) from 
    (
    select 
    adset_id as adgroup_id,
    adset_name as adgroup_name,
    'Facebook' as ad_channel,
    campaign_id, 
    'Facebook' as campaign_type, 
    campaign_name,
    date(date_start) as last_updated_date,
    row_number() over(partition by adset_id order by date(date_start) desc) row_num
    from {{ ref('FacebookAdinsights') }}
    )
where row_num = 1