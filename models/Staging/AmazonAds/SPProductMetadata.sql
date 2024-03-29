{% if var('AMAZONSPADS') and var('SPProductMetadata',False) %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('sp_productmetadata_tbl_ptrn','%sponsoredproducts%productmetadata'),
exclude=var('sp_productmetadata_tbl_exclude_ptrn',''),
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
    '{{brand|replace("`","")}}' as brand,
    '{{store|replace("`","")}}' as store,
    RequestTime,			
    profileId,
    countryName,
    accountName,
    accountId,
    fetchDate,
    eligibilityStatus,
    {{extract_nested_value("basisPrice","amount","numeric")}} as basisPrice_amount,
    {{extract_nested_value("basisPrice","currency","string")}} as basisPrice_currency,
    case 
        when createdDate='' then Null
        else
            {% if target.type == 'snowflake'%}
                TO_DATE(createdDate, 'Mon DD, YYYY')
            {% else %}  
                PARSE_DATE('%b %d, %Y', createdDate) 
            {% endif %}    
    end as createdDate,
    imageUrl,
    {{extract_nested_value("priceToPay","amount","numeric")}} as priceToPay_amount,
    {{extract_nested_value("priceToPay","currency","string")}} as priceToPay_currency,
    asin,
    availability,
    sku,
    title,
    variationList,
    ineligibilityReasons,
    ineligibilityCodes,
     {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then {{extract_nested_value("basisPrice","currency","string")}}  else c.from_currency_code end as exchange_currency_code,
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        {{extract_nested_value("basisPrice","currency","string")}} as exchange_currency_code,
    {% endif %}
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
    {{unnesting("basisPrice")}}
    {{unnesting("priceToPay")}}
    
        {% if var('currency_conversion_flag') %} 
            left join {{ref('ExchangeRates')}} c on date(a.RequestTime) = c.date and {{extract_nested_value("priceToPay","currency","string")}} = c.to_currency_code
        {% endif %}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sp_productmetadata_lookback',2592000000) }},0) from {{ this }})
        {% endif %}    
        qualify row_number() over (partition by profileId,countryName,accountName,accountId,fetchDate,asin,sku order by a.{{daton_batch_runtime()}} desc) = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}   
