{% macro store_name(variable) %}
    case
    when {{variable}} = 'Amazon.com.mx' or {{variable}} = 'MX' then 'Mexico'
    when {{variable}} = 'Amazon.com.br' or {{variable}} = 'BR' then 'Brazil'
    when {{variable}}  = 'Amazon.co.uk' or {{variable}}  = 'GB' or {{variable}}  = 'UK' or {{variable}}  = 'SI UK Prod Marketplace' then 'United Kingdom'
    when {{variable}}  = 'Amazon.com' or {{variable}}  = 'USD' or {{variable}}  = 'US' then 'United States'
    when {{variable}}  = 'Amazon.ca' or {{variable}}  = 'CAD' or {{variable}}  = 'CA' or {{variable}}  = 'SI CA Prod Marketplace' then 'Canada'
    when {{variable}}  = 'INR' then 'India'
    when {{variable}}  = 'FR' then 'France'
    when {{variable}}  = 'ES' then 'Spain'
    when {{variable}}  = 'DE' then 'Germany'
    when {{variable}}  = 'IT' then 'Italy'
    else {{variable}}  end as store_name
{% endmacro %}