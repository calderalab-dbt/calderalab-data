{% macro multi_unnesting_new(variable, variable1) %}
 
    {% if target.type =='snowflake' %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}}.VALUE:{{variable1}}),outer => true) {{variable1}}
    {% else %}
    left join unnest({{variable}}.{{variable1}}) {{variable}}_{{variable1}}
    {% endif %}
   
{% endmacro %}