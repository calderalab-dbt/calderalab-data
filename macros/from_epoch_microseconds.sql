{% macro from_epoch_microseconds(variable) %}

    {% if target.type =='snowflake' %}
        to_timestamp_ntz(cast({{variable}} as int))
    {% else %}
        TIMESTAMP_MICROS(cast({{variable}} as int))
    {% endif %}

{% endmacro %}


