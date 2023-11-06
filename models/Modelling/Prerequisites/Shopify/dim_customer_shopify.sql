select * {{exclude()}} (row_num)
from (
    select *,
    row_number() over(partition by customer_id order by last_updated_date desc) row_num
    from 
        (
        select
        coalesce(customer_id,'') as customer_id,
        coalesce(email,'') as email,
        'Shopify' as acquisition_channel,
        date(created_at) as order_date,
        -- cast(order_id as string) as order_id,
        cast(updated_at as date) as last_updated_date
        from {{ ref('ShopifyOrdersCustomer') }}
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
        {% endif %}

        union all

        select
        coalesce(customers_id,'') as customer_id,
        coalesce(email,'') as email, 
        'Shopify' as acquisition_channel,
        cast(null as date) as order_date,
        -- cast(null as string) as order_id,
        cast(updated_at as date) as last_updated_date
        from {{ ref('ShopifyCustomers') }}
        where 
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }} and
        {% endif %}
        customers_id not in (select distinct customer_id from {{ ref('ShopifyOrdersCustomer') }})
        ) 
    ) 
where row_num = 1

