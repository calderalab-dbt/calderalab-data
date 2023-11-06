{% macro from_epoch_milliseconds_gen(variable) %}

    {% if target.type =='snowflake' %}
        to_timestamp_ntz(cast({{variable}} as int))
    {% else %}
        TIMESTAMP_MILLIS(cast({{variable}} as int))
    {% endif %}

{% endmacro %}


