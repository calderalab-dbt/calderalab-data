{% macro nested_col(variable,variable1) %}
    {% if target.type =='snowflake' %}
     {{variable}}.VALUE:{{variable1}}
    {% else %}
     {{variable}}.{{variable1}}
    {% endif %}
    
{% endmacro %}