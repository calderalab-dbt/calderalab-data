-- depends_on: {{ ref('dim_product_shopify') }}
-- Returns a list of relations that match schema_pattern.table_pattern%
{% set relations = dbt_utils.get_relations_by_pattern(var('prerequisite_mdl_schema'), 'dim_product_%') %}

select * {{exclude()}} (row_num, _daton_batch_runtime) from ( 
select *, row_number() over(partition by product_id,sku,platform_name, start_date, end_date order by _daton_batch_runtime desc) row_num from (
{% for i in relations %}
        select 
        {{ dbt_utils.surrogate_key(['product_id','sku','platform_name']) }} AS product_key,
        platform_name,
        product_name,
        product_id,
        sku,
        color,
        seller,
        size,
        product_category,
        description, 
        category, 
        sub_category, 
        mrp, 
        cogs, 
        start_date, 
        end_date,
        _daton_batch_runtime
	    from {{i}}
    {% if not loop.last %} union all {% endif %}
        
{% endfor %}
)) where row_num = 1