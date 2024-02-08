{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select
'Amazon Seller Central' as platform_name,
a.amazon_order_id as order_id,
a.sku,
b.asin1 as product_id,
a.shipment_id,
a.shipment_item_id,
'Amazon' as fulfillment_channel,
a.fulfillment_center_id,
cast(null as date) as shipment_date,
a.carrier,
cast(substring(a.estimated_arrival_date,1,10) as date) estimated_delivery_date,
cast(null as date) delivered_at,
a.tracking_number,
date(a.reporting_date) as last_updated_date,
from {{ ref('FBAAmazonFulfilledShipmentsReport') }} a
left join (select distinct asin1, seller_sku from {{ ref('AllListingsReport')}}) b 
on a.sku = b.seller_sku
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE date(a.reporting_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(a.reporting_date)") }}
{% endif %}