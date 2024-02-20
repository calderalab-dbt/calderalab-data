{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select 
brand,
marketplaceName,
asin, 
sku,
snapshot_date,
available,
inv_age_0_to_90_days,
inv_age_91_to_180_days,
inv_age_181_to_270_days,
inv_age_271_to_365_days,
inv_age_365_plus_days,
your_price,
sales_price,	
sales_rank,
days_of_supply,
units_shipped_t7,
units_shipped_t30,
units_shipped_t60,
units_shipped_t90,
healthy_inventory_level,
inbound_quantity,
inbound_working,
inbound_shipped,
inbound_received
from {{ ref('FBAManageInventoryHealthReport') }}