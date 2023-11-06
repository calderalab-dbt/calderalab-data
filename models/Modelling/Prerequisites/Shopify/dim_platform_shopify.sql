select * {{exclude()}} (row_num) from 
    (
    select
    'Shopify' as platform_name,
    -- cast(null as string) type,
    source_name as type,
    {{ store_name('store') }},
    cast(null as string) description,
    cast(null as string) status,
    date(updated_at) as last_updated_date,
    row_number() over(partition by store order by date(updated_at) desc) row_num
    from {{ref('ShopifyOrders')}} 
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
    {% endif %}
    )
where row_num = 1

