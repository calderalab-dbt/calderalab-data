{% if var("AMAZONVENDOR") and var("RetailProcurementOrdersStatus",True) %} 
{{ config(enabled=True) }}
{% else %} {{ config(enabled=False) }}
{% endif %}
  
{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('RetailProcurementOrdersStatus_tbl_ptrn','%retailprocurementordersstatus%'),
exclude=var('RetailProcurementOrdersStatus_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}


     select 
            c.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),        
            {{ currency_conversion('b.value','b.from_currency_code','c.itemStatus_netCost_currencyCode') }},
            c._daton_user_id,
            c._daton_batch_runtime,
            c._daton_batch_id

    from 
    (
    
            select
            '{{brand|replace("`","")}}' as brand,
            '{{store|replace("`","")}}' as store,
            {{timezone_conversion('requeststartdate')}} as requeststartdate,
            {{timezone_conversion('requestenddate')}} as requestenddate,
            marketplacename,
            marketplaceid,
            vendorid,
            purchaseordernumber,
            purchaseorderstatus,
            {{timezone_conversion('purchaseOrderDate')}} as purchaseOrderDate,
            {{timezone_conversion('lastupdateddate')}} as lastupdateddate,
            {{extract_nested_value("sellingParty","partyId","string")}} as sellingParty_partyid,
            {{extract_nested_value("sellingParty_address","name","string")}} as sellingParty_address_name,
            {{extract_nested_value("sellingParty_address","addressLine1","string")}} as sellingParty_address_addressLine1,
            {{extract_nested_value("sellingParty_address","addressLine2","string")}} as sellingParty_address_addressLine2,
            {{extract_nested_value("sellingParty_address","addressLine3","string")}} as sellingParty_address_addressLine3,
            {{extract_nested_value("sellingParty_address","city","string")}} as sellingParty_address_city,
            {{extract_nested_value("sellingParty_address","county","string")}} as sellingParty_address_county,
            {{extract_nested_value("sellingParty_address","district","string")}} as sellingParty_address_district,
            {{extract_nested_value("sellingParty_address","stateOrRegion","string")}} as sellingParty_address_stateOrRegion,
            {{extract_nested_value("sellingParty_address","postalCode","string")}} as sellingParty_address_postalCode,
            {{extract_nested_value("sellingParty_address","countryCode","string")}} as sellingParty_address_countryCode,
            {{extract_nested_value("sellingParty_address","phone","string")}} as sellingParty_address_phone,
            {{extract_nested_value("sellingParty_taxInfo","taxRegistrationType","string")}} as sellingParty_taxInfo_taxRegistrationType,
            {{extract_nested_value("sellingParty_taxInfo","taxRegistrationNumber","string")}} as sellingParty_taxInfo_taxRegistrationNumber,
            {{extract_nested_value("shipToParty","partyId","string")}} as shipToParty_partyId,
            {{extract_nested_value("shipToParty_address","name","string")}} as shipToParty_address_name,
            {{extract_nested_value("shipToParty_address","addressLine1","string")}} as shipToParty_address_addressLine1,
            {{extract_nested_value("shipToParty_address","addressLine2","string")}} as shipToParty_address_addressLine2,
            {{extract_nested_value("shipToParty_address","addressLine3","string")}} as shipToParty_address_addressLine3,
            {{extract_nested_value("shipToParty_address","city","string")}} as shipToParty_address_city,
            {{extract_nested_value("shipToParty_address","county","string")}} as shipToParty_address_county,
            {{extract_nested_value("shipToParty_address","district","string")}} as shipToParty_address_district,
            {{extract_nested_value("shipToParty_address","stateOrRegion","string")}} as shipToParty_address_stateOrRegion,
            {{extract_nested_value("shipToParty_address","postalCode","string")}} as shipToParty_address_postalCode,
            {{extract_nested_value("shipToParty_address","countryCode","string")}} as shipToParty_address_countryCode,
            {{extract_nested_value("shipToParty_address","phone","string")}} as shipToParty_address_phone,
            {{extract_nested_value("shipToParty_taxInfo","taxRegistrationType","string")}} as shipToParty_taxInfo_taxRegistrationType,
            {{extract_nested_value("shipToParty_taxInfo","taxRegistrationNumber","string")}} as shipToParty_taxInfo_taxRegistrationNumber,
            {{extract_nested_value("itemStatus","itemSequenceNumber","string")}} as itemStatus_itemSequenceNumber,
            {{extract_nested_value("itemStatus","buyerProductIdentifier","string")}} as itemStatus_buyerProductIdentifier,
            {{extract_nested_value("itemStatus","vendorProductIdentifier","string")}} as itemStatus_vendorProductIdentifier,
            {{extract_nested_value("itemStatus_netCost","currencyCode","string")}} as itemStatus_netCost_currencyCode,
            {{extract_nested_value("itemStatus_netCost","amount","numeric")}} as itemStatus_netCost_amount,
            {{extract_nested_value("itemStatus_listPrice","currencyCode","string")}} as itemStatus_listPrice_currencyCode,
            {{extract_nested_value("itemStatus_listPrice","amount","numeric")}} as itemStatus_listPrice_amount,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantity","amount","integer")}} as itemStatus_orderedQuantity_orderedQuantity_amount,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantity","unitOfMeasure","string")}} as itemStatus_orderedQuantity_orderedQuantity_unitOfMeasure,            
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantity","unitSize","integer")}} as itemStatus_orderedQuantity_orderedQuantity_unitSize,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails","updatedDate","timestamp")}} as itemStatus_orderedQuantity_orderedQuantityDetails_updatedDate,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity","amount","integer")}} as itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity_amount,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity","unitOfMeasure","string")}} as itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity_unitOfMeasure,                            
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity","unitSize","integer")}} as itemStatus_orderedQuantity_orderedQuantityDetails_orderedQuantity_unitSize,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity","amount","integer")}} as itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity_amount,
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity","unitOfMeasure","string")}} as itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity_unitOfMeasure,            
            {{extract_nested_value("itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity","unitSize","integer")}} as itemStatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity_unitSize,
            {{extract_nested_value("itemStatus_acknowledgementStatus","confirmationStatus","string")}} as itemStatus_acknowledgementStatus_confirmationStatus,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acceptedQuantity","amount","integer")}} as itemStatus_acknowledgementStatus_acceptedQuantity_amount,            
            {{extract_nested_value("itemStatus_acknowledgementStatus_acceptedQuantity","unitOfMeasure","string")}} as itemStatus_acknowledgementStatus_acceptedQuantity_unitOfMeasure,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acceptedQuantity","unitSize","integer")}} as itemStatus_acknowledgementStatus_acceptedQuantity_unitSize,
            {{extract_nested_value("itemStatus_acknowledgementStatus_rejectedQuantity","amount","integer")}} as itemStatus_acknowledgementStatus_rejectedQuantity_amount,            
            {{extract_nested_value("itemStatus_acknowledgementStatus_rejectedQuantity","unitOfMeasure","string")}} as itemStatus_acknowledgementStatus_rejectedQuantity_unitOfMeasure,
            {{extract_nested_value("itemStatus_acknowledgementStatus_rejectedQuantity","unitSize","integer")}} as itemStatus_acknowledgementStatus_rejectedQuantity_unitSize,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails","acknowledgementDate","timestamp")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acknowledgementDate,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity","amount","integer")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity_amount,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity","unitofMeasure","string")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity_unitofMeasure,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity","unitSize","integer")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_acceptedQuantity_unitSize,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity","amount","integer")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity_amount,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity","unitofMeasure","string")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity_unitofMeasure,
            {{extract_nested_value("itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity","unitSize","integer")}} as itemStatus_acknowledgementStatus_acknowledgementStatusDetails_rejectedQuantity_unitSize,
            {{extract_nested_value("itemStatus_receivingStatus","receiveStatus","string")}} as itemStatus_receivingStatus_receiveStatus,
            {{extract_nested_value("itemStatus_receivingStatus_receivedQuantity","amount","integer")}} as itemStatus_receivingStatus_receivedQuantity_amount,
            {{extract_nested_value("itemStatus_receivingStatus_receivedQuantity","unitOfMeasure","string")}} as itemStatus_receivingStatus_receivedQuantity_unitOfMeasure,
            {{extract_nested_value("itemStatus_receivingStatus_receivedQuantity","unitSize","integer")}} as itemStatus_receivingStatus_receivedQuantity_unitSize,
            {{extract_nested_value("itemStatus_receivingStatus","lastReceiveDate","timestamp")}} as itemStatus_receivingStatus_lastReceiveDate,
            
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from
            {{ i }} a

            {{ unnesting("itemstatus") }}
            {{ unnesting("sellingParty") }}
            {{ unnesting("shipToParty") }}
            {{ multi_unnesting_new("sellingParty", "address") }} as sellingParty_address
            {{ multi_unnesting_new("sellingParty", "taxInfo") }} as sellingParty_taxInfo
            {{ multi_unnesting_new("shipToParty", "address") }} as shipToParty_address
            {{ multi_unnesting_new("shipToParty", "taxInfo") }} as shipToParty_taxInfo
            {{ multi_unnesting_new("itemstatus", "netcost") }} as itemstatus_netcost
            {{ multi_unnesting_new("itemstatus", "listprice") }} as itemstatus_listprice
            {{ multi_unnesting_new("itemstatus", "orderedquantity") }} as itemstatus_orderedquantity
            {{ multi_unnesting_new("itemstatus", "acknowledgementstatus") }} as itemstatus_acknowledgementstatus
            {{ multi_unnesting_new("itemstatus_orderedQuantity", "orderedQuantity") }} as itemstatus_orderedQuantity_orderedQuantity
            {{ multi_unnesting_new("itemstatus_orderedQuantity", "orderedQuantityDetails") }} as itemstatus_orderedQuantity_orderedQuantityDetails
            {{ multi_unnesting_new("itemstatus_orderedQuantity_orderedQuantityDetails", "orderedQuantity") }} as itemstatus_orderedQuantity_orderedQuantityDetails_orderedQuantity
            {{ multi_unnesting_new("itemstatus_orderedQuantity_orderedQuantityDetails", "cancelledQuantity") }} as itemstatus_orderedQuantity_orderedQuantityDetails_cancelledQuantity
            {{ multi_unnesting_new("itemstatus_acknowledgementstatus", "acknowledgementStatusDetails") }} as itemstatus_acknowledgementstatus_acknowledgementStatusDetails
            {{ multi_unnesting_new("itemstatus_acknowledgementstatus_acknowledgementStatusDetails", "acceptedQuantity") }} as itemstatus_acknowledgementstatus_acknowledgementStatusDetails_acceptedQuantity
            {{ multi_unnesting_new("itemstatus_acknowledgementstatus_acknowledgementStatusDetails", "rejectedQuantity") }} as itemstatus_acknowledgementstatus_acknowledgementStatusDetails_rejectedQuantity
            {{ multi_unnesting_new("itemstatus_acknowledgementstatus", "acceptedquantity") }} as itemstatus_acknowledgementstatus_acceptedquantity
            {{ multi_unnesting_new("itemstatus_acknowledgementstatus", "rejectedquantity") }} as itemstatus_acknowledgementstatus_rejectedquantity
            {{ multi_unnesting_new("itemstatus", "receivingStatus") }} as itemstatus_receivingStatus
            {{ multi_unnesting_new("itemstatus_receivingStatus", "receivedQuantity") }} as itemstatus_receivingStatus_receivedQuantity
    ) c
        {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} b on date(requeststartdate) = b.date and itemStatus_netCost_currencyCode = b.to_currency_code
                
            {% endif %}

            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE c._daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('RetailProcurementOrdersStatus_lookback',172800000) }},0) from {{ this }})
            {% endif %}
        qualify dense_rank() over (partition by purchaseordernumber, purchaseorderdate, purchaseOrderStatus,itemStatus_buyerProductIdentifier 
        order by itemStatus_orderedQuantity_orderedQuantityDetails_updatedDate desc, c._daton_batch_runtime  desc) = 1
        {% if not loop.last %} union all {% endif %}
{% endfor %}       