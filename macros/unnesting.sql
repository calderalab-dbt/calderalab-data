{% macro unnesting(variable) %}
    {% if target.type =='snowflake' %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}}),outer => true) {{variable}}
    {% else %}
    left join unnest({{variable}}) {{variable}}
    {% endif %}
    
{% endmacro %}