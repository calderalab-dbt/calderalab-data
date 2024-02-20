-- depends_on: {{ ref('dim_ads_amazonsp') }}
-- depends_on: {{ ref('dim_ads_amazonsd') }}
-- depends_on: {{ ref('dim_ads_amazonsb') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_ads_%') %}

{% for i in relations %}
        select  
        {{ dbt_utils.generate_surrogate_key(['ad_id', 'ad_type']) }} AS ad_key,
        {{ dbt_utils.generate_surrogate_key(['adgroup_id','campaign_type']) }} AS adgroup_key,
        {{ dbt_utils.generate_surrogate_key(['campaign_id','campaign_type']) }} AS campaign_key, 
        ad_id, 
        ad_channel,
        ad_name, 
        ad_type,
        last_updated_date as effective_start_date,
        cast(null as date) as effective_end_date,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}