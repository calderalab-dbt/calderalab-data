{% if var('AMAZONSPADS') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select 
    prodads.campaignId as campaign_id, 
    'Sponsored Products' as campaign_type, 
    campaignName as campaign_name, 
    coalesce(prodads.portfolioId,'') as portfolio_id,
    coalesce(name,'') as portfolio_name,
    'Amazon' as ad_channel, 
    campaignStatus as status, 
    campaignBudget as budget, 
    campaignBudgetType as budget_type,
    coalesce(placement,cast(null as string)) as campaign_placement,
    cast(null as decimal) as bidding_amount, 
    cast(null as string) as bidding_strategy_type,
    reportDate as last_updated_date,
    row_number() over(partition by prodads.campaignId order by _daton_batch_runtime desc) row_num
    from {{ ref('SPProductAdsReport') }} prodads
    left join (select distinct campaignId, placement from {{ ref('SPPlacementCampaignsReport') }}) pl
    on prodads.campaignId = pl.campaignId
    left join (select distinct campaign.portfolioId, portfolio.name, campaign.campaignId
    from {{ ref('SPCampaign') }} campaign
    left join {{ ref('SPPortfolio') }} portfolio
    on cast(campaign.portfolioId as string) = portfolio.portfolioId
    ) portfolio_map on prodads.campaignId = portfolio_map.campaignId
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
    {% endif %}
    )
where row_num = 1