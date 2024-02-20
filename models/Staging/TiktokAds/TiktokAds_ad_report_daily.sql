
{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%caldera_tiktok_connector_v1_ad_report_daily%')}}    
{% endset %}

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}


    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        AdID,
        Date,
        Cost,
        CPC,
        CPM,
        COALESCE(Impression,Impression_in) Impression,
        Click,
        CTR,
        Reach,
        Costper1000PeopleReached,
        COALESCE(Conversions,Conversions_in) Conversions,
        CPA,
        CVR,
        RealtimeConversions,
        RealtimeCPA,
        RealtimeCVR,
        Results,
        CostPerResults,
        ResultsRate,
        RealtimeResults,
        RealtimeCostPerResults,
        RealtimeResultsRate,
        SecondaryGoalResult,
        CostperSecondaryGoalResult,
        SecondaryGoalResultRatepercent,
        PaidLikes,
        PaidComments,
        PaidShares,
        PaidProfileVisits,
        PaidFollowers,
        Videoviews,
        _daton_pre_2SecondVideoViews,
        _daton_pre_6SecondVideoViews,
        VideoViewsat25percent,
        VideoViewsat50percent,
        VideoViewsat75percent,
        VideoViewsat100percent,
        AverageWatchTimeperVideoView,
        AverageWatchTimeperPerson,
        UniqueGenerateLead,
        TotalPurchase,
        TotalPurchaseValue,
        TotalGenerateLead,
        TotalGenerateLeadValue,
        TotalCompletePayment,
        ValueperCompletePayment,
        TotalCompletePaymentValue,
        CPCDestination,
        COALESCE(ClicksDestination,Clicks,Click) ClicksDestination,
        CTRDestination,
        Clicks,
        -- AccountName,
        Campaignname,
        CampaignID,
        AdGroupName,
        AdgroupID,
        AdName,
        Objective,
        PromotionType,
        PlacementsTypes,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        Row_number() OVER (PARTITION BY Date, AdID order by {{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}



