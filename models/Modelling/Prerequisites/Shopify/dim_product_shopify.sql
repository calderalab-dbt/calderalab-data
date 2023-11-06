
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

select 
case when prod.product_id is not null and prod.sku is not null then prod.platform_name else ord_prod.platform_name end as platform_name,
case when prod.product_id is not null and prod.sku is not null then prod.product_id else ord_prod.product_id end as product_id,
case when prod.product_id is not null and prod.sku is not null then prod.sku else ord_prod.sku end as sku,
case when prod.product_id is not null and prod.sku is not null then prod.product_name else ord_prod.product_name end as product_name,
case when prod.product_id is not null and prod.sku is not null then prod.color else ord_prod.color end as color,
case when prod.product_id is not null and prod.sku is not null then prod.seller else ord_prod.seller end as seller,
case when prod.product_id is not null and prod.sku is not null then prod.size else ord_prod.size end as size,
case when prod.product_id is not null and prod.sku is not null then prod.product_category else ord_prod.product_category end as product_category,
case when prod.product_id is not null and prod.sku is not null then prod._daton_batch_runtime else ord_prod._daton_batch_runtime end as _daton_batch_runtime,
{% if var('product_details_gs_flag') %}
description, 
category, 
sub_category, 
cast(mrp as numeric) mrp, 
cast(cogs as numeric) cogs, 
currency_code,
cast(start_date as date) date, 
cast(end_date as  date) date
{% else %}
cast(null as string) as description, 
-- cast(null as string) as category, 
case when prod.product_id is not null and prod.sku is not null then prod.product_category else ord_prod.product_category end as category,
cast(null as string) as sub_category, 
cast(null as numeric) as mrp, 
cast(null as numeric) as cogs, 
cast(null as string) as currency_code,
cast(null as date) as start_date, 
cast(null as date) as end_date 
{% endif %} 
from (
    select distinct
    'Shopify' as platform_name,
    coalesce(cast(id as string),'') as product_id,
    coalesce(variants_sku, '') as sku,
    title as product_name, 
    cast(null as string) as color, 
    vendor as seller,
    cast(null as string) as size,
    product_type as product_category,
    _daton_batch_runtime,
    updated_at
    from {{ ref('ShopifyProducts') }} ) prod

    full join

    ( select * from 
    (select
    'Shopify' as platform_name,
    cast(line_items_product_id as string) as product_id,
    coalesce(line_items_sku,'') as sku,
    line_items_title as product_name, 
    cast(null as string) as color, 
    vendor as seller,
    cast(null as string) as size,
    cast(null as string) product_category,
    _daton_batch_runtime,
    row_number() over(partition by line_items_product_id, line_items_sku order by _daton_batch_runtime desc) as row_num,
    updated_at,
    from {{ ref('ShopifyOrdersLineItems') }} ) x where row_num=1) ord_prod
    
    on prod.product_id=ord_prod.product_id and prod.sku=ord_prod.sku

{% if var('product_details_gs_flag') %}
left join (
    select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
    from {{ ref('ProductDetails') }} 
    where lower(platform_name) = 'shopify') prod_gs
on prod.sku = prod_gs.sku
{% endif %}
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(prod.updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(prod.updated_at)") }}
        and date(ord_prod.updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ord_prod.updated_at)") }}
    {% endif %}




