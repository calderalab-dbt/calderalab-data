<<<<<<< HEAD
-- depends_on: {{ ref('fact_advertising_amazonsp') }}
-- depends_on: {{ ref('fact_advertising_amazonsd') }}
-- depends_on: {{ ref('fact_advertising_amazonsb') }}
=======
-- depends_on: {{ ref('fact_advertising_facebookads') }}
-- depends_on: {{ ref('fact_advertising_googleads') }}
-- depends_on: {{ ref('fact_advertising_youtubeads') }}
-- depends_on: {{ ref('fact_advertising_tiktokads') }}

>>>>>>> f409855fb745c9264dcf6ed5d29148ec82b70a29

-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'fact_advertising%') %}

{% for i in relations %}
    select
<<<<<<< HEAD
    {{ dbt_utils.generate_surrogate_key(['campaign_id','campaign_type'])}} AS campaign_key,
    {{ dbt_utils.generate_surrogate_key(['adgroup_id','campaign_type'])}} AS adgroup_key,
    {{ dbt_utils.generate_surrogate_key(['ad_id', 'ad_type']) }} AS ad_key,
    {{ dbt_utils.generate_surrogate_key(['flow_id','ad_channel']) }} AS flow_key,
    {{ dbt_utils.generate_surrogate_key(['brand'])}} AS brand_key,
    {{ dbt_utils.generate_surrogate_key(['platform_name','store_name'])}} AS platform_key,
    {{ dbt_utils.generate_surrogate_key(['product_id','sku','platform_name'])}} AS product_key,
    date,
=======
    campaign_id as campaign_key,
    {{ dbt_utils.surrogate_key(['adgroup_id','campaign_type'])}} AS adgroup_key,
    {{ dbt_utils.surrogate_key(['ad_id', 'ad_type']) }} AS ad_key,
    {{ dbt_utils.surrogate_key(['flow_id','ad_channel']) }} AS flow_key,
    {{ dbt_utils.surrogate_key(['brand'])}} AS brand_key,
    {{ dbt_utils.surrogate_key(['platform_name','store_name'])}} AS platform_key,
    {{ dbt_utils.surrogate_key(['product_id','sku','platform_name','variant_id'])}} AS product_key,
    date,
    ad_channel,
>>>>>>> f409855fb745c9264dcf6ed5d29148ec82b70a29
    clicks,
    impressions,
    conversions,
    email_deliveries,
    email_opens,
    email_unsubscriptions,
    round((spend/exchange_currency_rate),2) as spend,
    round((sales/exchange_currency_rate),2) as sales,
    quantity as sold_quantity, 
    exchange_currency_code as currency_code,
    cast(null as int) as interactions,
    cast(null as decimal) as interaction_rate,
    cast(null as int) as engagements,
    cast(null as decimal) as engagement_rate,
    cast(null as int) as video_views,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}