{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetailsInsights') }}
{% endif %}

select distinct prod.*,
{% if var('product_details_gs_flag') %}
description, 
category, 
sub_category, 
cast(mrp as numeric) mrp, 
cast(cogs as numeric) cogs, 
currency_code,
cast(start_date as date) start_date, 
cast(end_date as  date) end_date
{% else %}
cast(null as string) as description, 
cast(null as string) as category, 
cast(null as string) as sub_category, 
cast(null as numeric) as mrp, 
cast(null as numeric) as cogs, 
cast(null as string) as currency_code,
cast(null as date) as start_date, 
cast(null as date) as end_date 
{% endif %} 

from (
select 
'Amazon Vendor Central' as platform_name,
asin as product_id,
cast(null as string) as sku,
cast(null as string) as product_name,
cast(null as string) as color, 
cast(null as string) as seller,
cast(null as string) as size,
cast(null as string) product_category,
cast(null as string) as image_url,
_daton_batch_runtime 
from {{ ref('VendorTrafficReport') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}

union all

select 
'Amazon Vendor Central' as platform_name,
itemstatus_buyerproductidentifier as product_id,
cast(null as string) as sku,
cast(null as string) as product_name,
cast(null as string) as color, 
cast(null as string) as seller,
cast(null as string) as size,
cast(null as string) as product_category,
cast(null as string) as image_url,
_daton_batch_runtime 
from {{ ref('RetailProcurementOrdersStatus') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE date(lastUpdatedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(lastUpdatedDate)") }}
{% endif %}
) prod

{% if var('product_details_gs_flag') %}
left join (
  select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
  from {{ ref('ProductDetailsInsights') }} 
  where lower(platform_name) = 'amazon vendor central') prod_gs
on prod.sku = prod_gs.sku
{% endif %}


