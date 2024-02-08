{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Seller Central' as platform_name,
    cast(null as string) type,
    {{ store_name('sales_channel') }},
    cast(null as string) description,
    cast(null as string) status,
    date(last_updated_date) as last_updated_date,
    row_number() over(partition by sales_channel order by date(last_updated_date) desc) row_num
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where date(last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(last_updated_date)") }}
    {% endif %}
    )
where row_num = 1

