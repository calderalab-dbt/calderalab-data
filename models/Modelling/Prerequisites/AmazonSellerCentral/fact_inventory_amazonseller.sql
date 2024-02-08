{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

with inventory_health as (
{{ dbt_utils.unpivot(ref('inventoryhealth_view'), cast_to='int', exclude=['brand','marketplaceName','asin','sku','snapshot_date']) }}
)

select 
brand,
'Amazon Seller Central' as order_platform,
{{ store_name('marketplaceName') }},
cast (null as string) as fulfillment_center_id,
'Amazon' as fulfillment_channel,
asin as product_id,
sku, 
snapshot_date as date,
'Customer Shipment' as event,
field_name as type,
value
from inventory_health
where value is not null