{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Vendor Central' as platform_name,
    sellingParty_partyid as vendor_id,
    cast(null as string) email,
    cast(null as string) full_name,
    cast(null as string) phone,
    date(lastupdateddate) as last_updated_date,
    row_number() over(partition by sellingParty_partyid order by _daton_batch_runtime desc) row_num
    from {{ref('RetailProcurementOrdersStatus')}} 
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(lastupdateddate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(lastupdateddate)") }}
    {% endif %}
    ) orders
where row_num = 1