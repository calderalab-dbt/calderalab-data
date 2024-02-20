{% if var('AMAZONSBADS') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
brand,
{{ store_name('countryName') }},
campaignId as campaign_id,
cast(null as string) as flow_id,
adGroupId as adgroup_id, 
cast(null as string) as ad_id,
cast(null as string) as product_id,
cast(null as string) as sku,
date(reportDate) as date,
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
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(attributedConversions14d) as conversions,
sum(unitsSold14d) as quantity,
sum(cost) as spend,
sum(attributedSales14d) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from (
select *,
{{currency_code('countryName')}} 
from {{ ref('SBUnifiedAdGroupsReport') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
{% endif %}
) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15