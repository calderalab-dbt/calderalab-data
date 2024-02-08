{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Vendor Central' as platform_name,
    cast(null as string) type,
    {{ store_name('marketplaceName') }},
    cast(null as string) description,
    cast(null as string) status,
    startdate as last_updated_date,
    row_number() over(partition by marketplaceName order by date(startdate) desc) row_num
    from {{ref('VendorSalesReportByManufacturing')}} 
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
    {% endif %}
    )
where row_num = 1

