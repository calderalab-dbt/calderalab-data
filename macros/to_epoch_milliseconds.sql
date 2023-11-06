{% macro to_epoch_milliseconds(variable) %}
    
    {% if target.type =='snowflake' %}
    DATE_PART(epoch_second, {{variable}})
    {% else %}
    unix_micros({{variable}}) 
    {% endif %}
    
{% endmacro %}