{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('sales_target_gs_flag') %}
-- depends_on: {{ ref('SalesTarget') }}
{% endif %}

select ord.*,
{% if var('sales_target_gs_flag') %}
  year,
  month,
  revenue_target,
  orders_target 
{% else %}
  extract(year from CURRENT_DATE()) year,
  extract(month from CURRENT_DATE()) month,
  cast(null as decimal) as revenue_target,
  cast(null as int) as orders_target  
{% endif %} 

from (
select 
brand as brand_name,
cast(null as string) as type,
cast(null as string) as description,
startdate as last_updated_date,
_daton_batch_runtime
from {{ ref('VendorSalesReportByManufacturing') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}
) ord

{% if var('sales_target_gs_flag') %}
  left join (select brand_name, year, month, revenue_target, orders_target from {{ ref('SalesTarget') }} 
  where lower(platform_name) = 'amazon vendor central') sales_gs
  on ord.brand_name = sales_gs.brand_name
{% endif %}


