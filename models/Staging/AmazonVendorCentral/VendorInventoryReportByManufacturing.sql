{% if var("AMAZONVENDOR") and var("VendorInventoryReportByManufacturing", True) %} 
    {{ config(enabled=True) }}
{% else %} 
    {{ config(enabled=False) }}
{% endif %}

{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('VendorInventoryReportByManufacturing_tbl_ptrn','%vendorinventoryreportbymanufacturing%'),
exclude=var('VendorInventoryReportByManufacturing_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

{# /*--iterating through all the tables */ #}

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
            {{ currency_conversion('b.value','b.from_currency_code','c.netReceivedInventoryCost_currencyCode') }},
            c._daton_user_id,
            c._daton_batch_runtime,
            c._daton_batch_id

    from 
    (


            select
            '{{brand|replace("`","")}}' as brand,
            '{{store|replace("`","")}}' as store,
            {{timezone_conversion('reportrequesttime')}} as reportrequesttime,
            vendorid,
            marketplacename,
            marketplaceid,
            {{timezone_conversion('startdate')}} as startdate,
            {{timezone_conversion('enddate')}} as enddate,
            asin,
            openpurchaseorderunits,
            averagevendorleadtimedays,
            sellthroughrate,
            unfilledcustomerorderedunits,
            {{extract_nested_value("netReceivedInventoryCost","amount","numeric")}} as netReceivedInventoryCost_amount,
            {{extract_nested_value("netReceivedInventoryCost","currencyCode","string")}}  as netReceivedInventoryCost_currencyCode,
            netreceivedinventoryunits,
            {{extract_nested_value("sellableOnHandInventoryCost","amount","numeric")}} as sellableOnHandInventoryCost_amount,
            {{extract_nested_value("sellableOnHandInventoryCost","currencyCode","string")}}  as sellableOnHandInventoryCost_currencyCode,
            sellableonhandinventoryunits,
            {{extract_nested_value("unsellableOnHandInventoryCost","amount","numeric")}} as unsellableOnHandInventoryCost_amount,
            {{extract_nested_value("unsellableOnHandInventoryCost","currencyCode","string")}}  as unsellableOnHandInventoryCost_currencyCode,
            unsellableonhandinventoryunits,
            {{extract_nested_value("aged90plusdayssellableinventorycost","amount","numeric")}} as aged90plusdayssellableinventorycost_amount,
            {{extract_nested_value("aged90plusdayssellableinventorycost","currencyCode","string")}}  as aged90plusdayssellableinventorycost_currencyCode,
            aged90plusdayssellableinventoryunits,
            {{extract_nested_value("unhealthyInventoryCost","amount","numeric")}} as unhealthyInventoryCost_amount,
            {{extract_nested_value("unhealthyInventoryCost","currencyCode","string")}}  as unhealthyInventoryCost_currencyCode,
            unhealthyInventoryUnits,
            procurableproductoutofstockrate,
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from
            {{ i }} a
            {{ unnesting("netreceivedinventorycost") }}
            {{ unnesting("sellableonhandinventorycost") }}
            {{ unnesting("unsellableonhandinventorycost") }}
            {{ unnesting("aged90PlusDaysSellableInventoryCost") }}
            {{ unnesting("unhealthyInventoryCost") }}
         ) c

            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} b on date(startdate) = b.date and netReceivedInventoryCost_currencyCode = b.to_currency_code

            {% endif %}  
             
                        {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        WHERE c._daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('VendorInventoryReportByManufacturing_lookback',172800000) }},0) from {{ this }})
                        {% endif %}
                    qualify dense_rank() over (partition by startdate, asin order by c._daton_batch_runtime desc) = 1
                {% if not loop.last %} union all {% endif %}
{% endfor %}
