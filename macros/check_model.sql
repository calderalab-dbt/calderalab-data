{% macro check_model(variable) %}

-- depends_on: {{ ref('SalesTarget') }}

{% for model in graph.nodes.values() if model.name.startswith('SalesTarget') %}
        SELECT * FROM {{ref(model.name)}}
        {% if not loop.last %} 
        UNION ALL
        {% endif %}  
{% endfor %}

{% endmacro %}
