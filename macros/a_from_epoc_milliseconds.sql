{% macro a_from_epoch_milliseconds() %}

    {% if target.type =='snowflake' %}
        to_timestamp_ntz(cast(a._daton_batch_runtime as int))
    {% else %}
        TIMESTAMP_MILLIS(cast(a._daton_batch_runtime as int))
    {% endif %}

{% endmacro %}
