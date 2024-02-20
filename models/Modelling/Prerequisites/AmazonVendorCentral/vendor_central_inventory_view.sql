{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

Select 
brand,
marketplaceName,
asin, 
cast(null as string) sku,
startdate as snapshot_date,
openPurchaseOrderUnits,
netReceivedInventoryUnits,
sellableOnHandInventoryUnits,
unsellableOnHandInventoryUnits,
aged90PlusDaysSellableInventoryUnits,
unhealthyInventoryUnits
from {{ ref('VendorInventoryReportByManufacturing') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE startdate >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="startdate") }}
{% endif %}

