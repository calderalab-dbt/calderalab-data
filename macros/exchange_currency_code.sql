{% macro exchange_currency_code(variable) %}
    case
    when {{variable}}  = 'Amazon.com' or {{variable}}  = 'United States' or {{variable}}  = 'US' then 'USD'
    when {{variable}}  = 'Amazon.ca' or {{variable}} = 'SI CA Prod Marketplace' or {{variable}}  = 'Canada' or {{variable}}  = 'CA' then 'CAD'
    when {{variable}}  = 'India' or {{variable}}  = 'IN' then 'INR'
    when {{variable}}  = 'France' or {{variable}}  = 'FR' then 'EUR'
    when {{variable}}  = 'Amazon.co.uk' or {{variable}}  = 'Great Britain' or {{variable}}  = 'United Kingdom' or {{variable}}  = 'GB' or {{variable}}  = 'UK' then 'GBP'
    when {{variable}}  = 'Amazon.com.mx' or {{variable}}  = 'Mexico' or {{variable}}  = 'MX' then 'MXN'
    when {{variable}}  = 'Germany' or {{variable}}  = 'DE' then 'EUR'
    when {{variable}}  = 'Spain' or {{variable}}  = 'ES' then 'EUR'
    when {{variable}}  = 'Italy' or {{variable}}  = 'IT' then 'EUR'
    when {{variable}}  = 'Amazon.com.br' or {{variable}}  = 'Brazil' or {{variable}}  = 'BR' then 'BRL'
    else {{variable}}  end 
{% endmacro %}
