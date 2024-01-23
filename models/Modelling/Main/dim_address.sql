-- depends_on: {{ ref('dim_address_shopify') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_address_%') %}

select * {{exclude()}} (row_num) from (
select *, row_number() over(partition by address_key order by effective_start_date desc) row_num
from (
{% for i in relations %}
    select  
    {{ dbt_utils.surrogate_key(['address_type','addr_line_1','addr_line_2','city','district','state','country','postal_code']) }} AS address_key,
    address_type,
    addr_line_1,
    addr_line_2,
    city,
    district,
    state,
    country,
    postal_code,
    last_updated_date as effective_start_date,
    cast(null as date) as effective_end_date,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)) where row_num = 1
