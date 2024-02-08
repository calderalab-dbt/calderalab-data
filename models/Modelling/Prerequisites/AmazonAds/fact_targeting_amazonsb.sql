{% if var('AMAZONSBADS') %}
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
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Amazon Seller Central' as platform_name,
'Amazon' as ad_channel,
'Sponsored Brands' as campaign_type,
'Sponsored Brands' as ad_type,
case when lower(matchType) in ('targeting_expression_predefined') then 'Automatic Targeting'
when lower(matchType) in ('broad','phrase','exact') then 'Manual Keyword Targeting'
else 'Others' end as targeting_type,
coalesce(cast(keywordId as string),'') targeting_id,
sum(cast(KeywordBid as numeric)) bid_amount,
sum(clicks) clicks,
sum(impressions) impressions,
sum(attributedConversions14d) as conversions,
sum(attributedConversions14d) as quantity,
sum(cost) as spend,
sum(attributedSales14d) as sales 
from (
select *,
{{currency_code('countryName')}} 
from {{ ref('SBUnifiedSearchTermKeywordsReport')}} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
{% endif %}
) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

union all 

select 
brand,
{{ store_name('countryName') }},
cast(campaignId as string) campaign_id,
adGroupId as adgroup_id,
cast(null as string) as ad_id,
date(reportDate) as date,
'' as search_term,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Amazon Seller Central' as platform_name,
'Amazon' as ad_channel,
'Sponsored Brands' as campaign_type,
'Sponsored Brands' as ad_type,
case when lower(targetingType) in ('targeting_expression_predefined') then 'Automatic Targeting'
when lower(targetingType) in ('targeting_expression') and lower(targetingText) like 'category%' then 'Manual Product Targeting' 
when lower(targetingType) in ('targeting_expression') and lower(targetingText) not like 'category%' then 'Manual Category Targeting'
else 'Others' end as targeting_type,
coalesce(cast(targetId as string),'') targeting_id,
sum(cast(null as numeric)) bid_amount,
sum(clicks) clicks,
sum(impressions) impressions,
sum(attributedConversions14d) as conversions,
sum(attributedConversions14d) as quantity,
sum(cost) as spend,
sum(attributedSales14d) as sales 
from (
select *,
{{currency_code('countryName')}} 
from {{ ref('SBUnifiedTargetReport')}} ) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15