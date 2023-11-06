select * from (
select 
'Shopify' as platform_name,
a.order_id,
cast(a.line_items_sku as string) as sku,
cast(a.line_items_product_id as string) as product_id,
fulfillments_id as shipment_id,
b.shipping_lines_id as shipment_item_id,
a.fulfillments_service as fulfillment_channel,
cast(null as string) as fulfillment_center_id,
cast(null as date) as shipment_date,
b.shipping_lines_carrier_identifier as carrier,
cast(null as date) estimated_delivery_date,
cast(null as date) delivered_at,
a.fulfillments_tracking_number as tracking_number,
date(a.updated_at) as last_updated_date,
row_number() over (partition by a.order_id,a.fulfillments_id,a.line_items_id order by date(a.updated_at) desc) as row_num
from {{ ref('ShopifyOrdersFulfillments') }} a
left join (select distinct order_id, shipping_lines_id, shipping_lines_carrier_identifier from {{ ref('ShopifyOrdersShippingLines')}}) b 
on a.order_id = b.order_id
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE date(a.updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(a.updated_at)") }}
{% endif %}
) x where row_num=1
