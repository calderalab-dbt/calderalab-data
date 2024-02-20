select * {{exclude()}} (row_num) from 
    (
    select 
    campaign_id, 
    'Facebook' as campaign_type, 
    campaign_name,
    account_id as portfolio_id,
    '' as portfolio_name,  
    'Facebook' as ad_channel, 
    '' as status,
    cast(null as numeric) as budget, 
    cast(null as string) as budget_type, 
    '' as campaign_placement,
    cast(null as decimal) as bidding_amount, 
    cast(null as string) as bidding_strategy_type,
    date(date_start) as last_updated_date,
    row_number() over(partition by campaign_id order by date(date_start) desc) as row_num 
    from {{ ref('FacebookAdinsights') }}
    )
where row_num = 1