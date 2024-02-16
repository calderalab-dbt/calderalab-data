-- depends_on: {{ ref('dim_brand_shopify') }}
-- depends_on: {{ ref('dim_brand_amazonseller') }}

-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_brand%') %}

select * {{exclude()}} (row_num) from (
select *, row_number() over(partition by brand_name, year, month order by effective_start_date desc) row_num from (
{% for i in relations %}
        select
        {{ dbt_utils.generate_surrogate_key(['brand_name']) }} AS brand_key, 
        brand_name,
        type,
        case when extract(month from date(current_timestamp())) = month and extract(year from date(current_timestamp())) = year then 'Active' 
        else 'Non-Active' end as status,
        description,
        year,
        month,
        revenue_target,
        orders_target, 
        last_updated_date as effective_start_date,
        cast(null as date) as effective_end_date,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	    from {{i}} 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)) where row_num = 1
 