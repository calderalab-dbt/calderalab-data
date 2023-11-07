-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_platform_%') %}

{% for i in relations %}
    select 
    {{ dbt_utils.surrogate_key(['platform_name','store_name']) }} AS platform_key, 
    platform_name,
    type,
    store_name,
    description,
    status,
    last_updated_date as effective_start_date,
    cast(null as date) as effective_end_date,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}




