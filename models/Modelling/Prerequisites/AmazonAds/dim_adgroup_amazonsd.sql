{% if var('AMAZONSDADS') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select distinct * {{exclude()}} (row_num) from (
    select 
    adGroupId as adgroup_id,
    adGroupName as adgroup_name,
    campaignId as campaign_id, 
    'Sponsored Display' as campaign_type, 
    'Amazon' as ad_channel,
    reportDate as last_updated_date,
    row_number() over(partition by adGroupId order by _daton_batch_runtime desc) row_num 
    from {{ ref('SDProductAdsReport') }}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE reportDate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="reportDate") }}
    {% endif %}
    )
where row_num = 1