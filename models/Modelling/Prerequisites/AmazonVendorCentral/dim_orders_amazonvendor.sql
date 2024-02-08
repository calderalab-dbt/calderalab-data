{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Vendor Central' as platform_name,
    purchaseordernumber as order_id,
    cast(null as string) as customer_id,
    cast(null as string) as payment_mode,
    'Online Marketplace' as order_channel,
    cast(null as integer) as rating,
    cast(null as string) as comments,
    date(lastupdateddate) as last_updated_date,
    row_number() over(partition by purchaseordernumber order by _daton_batch_runtime desc) row_num
    from {{ref('RetailProcurementOrdersStatus')}} 
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(lastUpdatedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(lastUpdatedDate)") }}
    {% endif %}
    )
where row_num = 1
