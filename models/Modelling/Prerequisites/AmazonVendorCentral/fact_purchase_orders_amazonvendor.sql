{% if var("AMAZONVENDOR") %} 
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

Select 
brand,
store_name,
platform_name,
order_date,
currency_code,
purchase_order_number,
product_id,
sku,
vendor_id,
sum(netcost_amount) netcost_amount,
sum(listprice_amount) listprice_amount,
sum(ordered_quantity_amount) ordered_quantity_amount,
sum(open_po_accepted_quantity_amount) open_po_accepted_quantity_amount,
sum(closed_po_accepted_quantity_amount) closed_po_accepted_quantity_amount,
sum(open_po_rejected_quantity_amount) open_po_rejected_quantity_amount,
sum(closed_po_rejected_quantity_amount) closed_po_rejected_quantity_amount,
sum(open_po_received_quantity_amount) open_po_received_quantity_amount,
sum(closed_po_received_quantity_amount) closed_po_received_quantity_amount
from (
    select
    brand,
    {{ store_name('marketplaceName') }},
    itemstatus_buyerproductidentifier as product_id,
    cast(null as string) as sku,
    sellingParty_partyId as vendor_id,
    cast(purchaseOrderDate as date) as order_date,
    exchange_currency_code as currency_code,
    'Amazon Vendor Central' as platform_name,
    purchaseOrderNumber as purchase_order_number,
    round((itemStatus_netCost_amount/exchange_currency_rate),2) as netcost_amount,
    round((itemStatus_listPrice_amount/exchange_currency_rate),2) as listprice_amount,
    round((itemStatus_orderedQuantity_orderedQuantity_amount/exchange_currency_rate),2) as ordered_quantity_amount,
    case when purchaseOrderStatus = 'OPEN' then itemStatus_acknowledgementStatus_acceptedQuantity_amount else 0 end as open_po_accepted_quantity_amount,
    case when purchaseOrderStatus = 'CLOSED' then itemStatus_acknowledgementStatus_acceptedQuantity_amount else 0 end as closed_po_accepted_quantity_amount,
    case when purchaseOrderStatus = 'OPEN' then itemStatus_acknowledgementStatus_rejectedQuantity_amount else 0 end as open_po_rejected_quantity_amount,
    case when purchaseOrderStatus = 'CLOSED' then itemStatus_acknowledgementStatus_rejectedQuantity_amount else 0 end as closed_po_rejected_quantity_amount,
    case when purchaseOrderStatus = 'OPEN' then itemStatus_acknowledgementStatus_rejectedQuantity_amount else 0 end as open_po_received_quantity_amount,
    case when purchaseOrderStatus = 'CLOSED' then itemStatus_acknowledgementStatus_rejectedQuantity_amount else 0 end as closed_po_received_quantity_amount
    from {{ ref('RetailProcurementOrdersStatus') }}
    {% if not flags.FULL_REFRESH %}
      {# /* -- this filter will only be applied on an incremental run */ #}
      WHERE date(lastUpdatedDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(lastUpdatedDate)") }}
    {% endif %}
    ) group by 1,2,3,4,5,6,7,8,9