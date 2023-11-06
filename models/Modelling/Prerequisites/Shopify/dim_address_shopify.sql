select * {{exclude()}} (row_num)
from (
    select
    order_id,
    platform_name,
    address_type,
    addr_line_1,
    addr_line_2,
    city,
    district,
    state,
    country,
    postal_code,
    last_updated_date,
    row_number() over(partition by address_type, addr_line_1, addr_line_2, city, state, district, country, postal_code  order by last_updated_date desc) row_num
    from 
        (
       

        select  
        order_id,
        'Shopify' as platform_name,
        'billing' as address_type,
        nullif(billing_address_address1, '') as addr_line_1,
        nullif(billing_address_address2, '') as addr_line_2,
        nullif(billing_address_city, '') as city,
        cast(null as string) as district,
        nullif(billing_address_province, '') as state,
        nullif(billing_address_country, '') as country, 
        nullif(billing_address_zip, '') as postal_code,
        -- date(updated_at) as effective_start_date,
        -- cast(null as date) as effective_end_date,
        date(updated_at) as last_updated_date,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{ ref('ShopifyOrdersAddresses') }}
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
        {% endif %}


        union all

        select  
        order_id,
        'Shopify' as platform_name,
        'shipping' as address_type,
        nullif(shipping_address_address1, '') as addr_line_1,
        nullif(shipping_address_address2, '') as addr_line_2,
        nullif(shipping_address_city, '') as city,
        cast(null as string) as district,
        nullif(shipping_address_province, '') as state,
        nullif(shipping_address_country, '') as country, 
        nullif(shipping_address_zip, '') as postal_code,
        -- date(updated_at) as effective_start_date,
        -- cast(null as date) as effective_end_date,
        date(updated_at) as last_updated_date,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{ ref('ShopifyOrdersAddresses') }}
        {% if not flags.FULL_REFRESH %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
        {% endif %}
        ) 
    ) 
where row_num = 1



        