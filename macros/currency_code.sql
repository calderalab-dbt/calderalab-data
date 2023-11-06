{% macro currency_code(variable) %}
    case
    when {{variable}}  = 'United States' or {{variable}}  = 'US' then 'USD'
    when {{variable}}  = 'Canada' or {{variable}}  = 'CA' then 'CAD'
    when {{variable}}  = 'India' or {{variable}}  = 'IN' then 'INR'
    when {{variable}}  = 'France' or {{variable}}  = 'FR' then 'EUR'
    when {{variable}}  = 'Great Britain' or {{variable}}  = 'United Kingdom' or {{variable}}  = 'GB' or {{variable}}  = 'UK' then 'GBP'
    when {{variable}}  = 'Mexico' or {{variable}}  = 'MX' then 'MXN'
    when {{variable}}  = 'Germany' or {{variable}}  = 'DE' then 'EUR'
    when {{variable}}  = 'Spain' or {{variable}}  = 'ES' then 'EUR'
    when {{variable}}  = 'Italy' or {{variable}}  = 'IT' then 'EUR'
    else {{variable}}  end as currency
{% endmacro %}