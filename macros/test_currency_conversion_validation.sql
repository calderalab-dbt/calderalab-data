{% macro test_currency_conversion_validation(model) %}

{% if var('currency_conversion_flag') %}
    select * from {{model}} where exchange_currency_rate is null or exchange_currency_code is null
{% else %}
    select * from {{model}} where exchange_currency_rate=1 or exchange_currency_code is not null
{% endif %}
{% endmacro %}