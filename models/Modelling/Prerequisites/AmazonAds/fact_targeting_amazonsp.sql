{% if var('AMAZONSPADS') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
brand,
{{ store_name('countryName') }},
cast(campaignId as string) campaign_id,
adGroupId as adgroup_id,
cast(null as string) as ad_id,
date(reportDate) as date,
coalesce(query,'') search_term,
exchange_currency_rate,
exchange_currency_code,
'Amazon Seller Central' as platform_name,
'Amazon' as ad_channel,
'Sponsored Products' as campaign_type,
'Sponsored Products' as ad_type,
case when lower(matchType) in ('targeting_expression_predefined') then 'Automatic Targeting'
when lower(matchType) in ('broad','phrase','exact') then 'Manual Keyword Targeting'
when lower(matchType) in ('targeting_expression') and lower(targetingText) like 'category%' then 'Manual Product Targeting' 
when lower(matchType) in ('targeting_expression') and lower(targetingText) not like 'category%' then 'Manual Category Targeting' 
else 'Others' end as targeting_type,
coalesce(cast(keywordId as string),'') targeting_id,
sum(cast(KeywordBid as numeric)) bid_amount,
sum(clicks) clicks,
sum(impressions) impressions,
sum(attributedConversions7d) as conversions,
sum(attributedUnitsOrdered7d) as quantity,
sum(cost) as spend,
sum(attributedSales7d) as sales 
from {{ ref('SPSearchTermKeywordReport')}}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15