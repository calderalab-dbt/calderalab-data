{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
* {{exclude()}}(row_num)
from (
    select  
    amazon_order_id as order_id,
    'Amazon Seller Central' as platform_name,
    'shipping' as address_type,
    ship_address_1 as addr_line_1,
    ship_address_2 as addr_line_2,
    ship_city as city,
    '' as district,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code,
    date(reporting_date) as last_updated_date,
    row_number() over(partition by amazon_order_id, ship_address_1, ship_address_2, ship_city, ship_state, ship_country, ship_postal_code  order by date(reporting_date) desc) row_num
    from {{ ref('FBAAmazonFulfilledShipmentsReport') }}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(reporting_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(reporting_date)") }}
    {% endif %}
    
    union all

    select  
    amazon_order_id as order_id,
    'Amazon Seller Central' as platform_name,
    'billing' as address_type,
    bill_address_1 as addr_line_1,
    ship_address_2 as addr_line_2,
    ship_city as city,
    '' as district,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code,
    date(reporting_date) as last_updated_date,
    row_number() over(partition by amazon_order_id, ship_address_1, ship_address_2, ship_city, ship_state, ship_country, ship_postal_code  order by date(reporting_date) desc) row_num
    from {{ ref('FBAAmazonFulfilledShipmentsReport') }}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(reporting_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(reporting_date)") }}
    {% endif %}
    ) 
where row_num = 1
