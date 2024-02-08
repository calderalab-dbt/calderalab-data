{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

with vendor_inventory_health as (
{{ dbt_utils.unpivot(ref('vendor_central_inventory_view'), cast_to='int', exclude=['brand','marketplaceName','asin','sku','snapshot_date']) }}
)

Select 
brand,
order_platform,
{{ store_name('marketplacename') }},
cast (null as string) as fulfillment_center_id,
'Amazon' as fulfillment_channel,
asin as product_id,
sku, 
date(snapshot_date) as date,
'Inventory' as event,
coalesce(field_name,'') as type,
value
from (
select *, 
'Amazon Vendor Central' as order_platform
from vendor_inventory_health)
where value is not null