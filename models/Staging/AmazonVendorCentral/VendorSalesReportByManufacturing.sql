{% if var("AMAZONVENDOR") and var("VendorSalesReportByManufacturing",True) %} 
    {{ config(enabled=True) }}
{% else %} 
    {{ config(enabled=False) }}
{% endif %}

{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('VendorSalesReportByManufacturing_tbl_ptrn','%vendorsalesreportbymanufacturing%'),
exclude=var('VendorSalesReportByManufacturing_exclude_tbl_ptrn',''),
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
        {{ currency_conversion('b.value','b.from_currency_code','c.orderedRevenue_currencyCode') }},
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
            customerReturns,
            {{extract_nested_value("orderedRevenue","amount","numeric")}} as orderedRevenue_amount,
            {{extract_nested_value("orderedRevenue","currencyCode","string")}}  as orderedRevenue_currencyCode,
            orderedUnits,
            {{extract_nested_value("shippedCogs","amount","numeric")}} as shippedCogs_amount,
            {{extract_nested_value("shippedCogs","currencyCode","string")}}  as shippedCogs_currencyCode,
            {{extract_nested_value("shippedRevenue","amount","numeric")}} as shippedRevenue_amount,
            {{extract_nested_value("shippedRevenue","currencyCode","string")}}  as shippedRevenue_currencyCode,
            shippedUnits,
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from

            {{ i }} a
            {{ unnesting("orderedRevenue") }}
            {{ unnesting("shippedCogs") }}
            {{ unnesting("shippedRevenue") }}
            
        ) c

        {% if var('currency_conversion_flag') %}    
                    left join {{ref('ExchangeRates')}} b on date(startdate) = b.date and orderedRevenue_currencyCode = b.to_currency_code
            {% endif %}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE c._daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('VendorSalesReportByManufacturing_lookback',172800000) }},0) from {{ this }})
        {% endif %}
        qualify dense_rank() over (partition by marketplaceId, startdate, asin order by c._daton_batch_runtime desc) = 1
        {% if not loop.last %} union all {% endif %}
{% endfor %}
