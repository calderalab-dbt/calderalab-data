{% set table_name_query %}
{{set_table_name_modelling('dim_address%')}} 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    select
    {{ dbt_utils.generate_surrogate_key(['order_id','platform_name']) }} AS order_key, 
    case when max(address_type)='shipping' then max({{ dbt_utils.generate_surrogate_key(['address_type','addr_line_1','addr_line_2','city','district','state','country','postal_code']) }}) else cast(null as string) end as ship_address_key,
    case when min(address_type)='billing' then max({{ dbt_utils.generate_surrogate_key(['address_type','addr_line_1','addr_line_2','city','district','state','country','postal_code']) }}) else cast(null as string) end as bill_address_key
    from {{i}}
    where address_type in ('shipping','billing')
    group by 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}