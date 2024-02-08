    {% if var('sales_target_gs_flag',False) %}
        {{ config( enabled = True ) }}
    {% else %}
        {{ config( enabled = False ) }}
    {% endif %}
    
    
    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        unique_key = ['platform_name','brand_name','year','month'])}}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
    {{set_table_name('%salestarget%monthly_target%')}}    
    {% endset %}  

    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}


    {% for i in results_list %}
        SELECT * {{exclude()}} (row_num)
        From (
            select 
            platform_name,	
            brand_name,	
            year,	
            month,	
            revenue_target,	
            orders_target,
            sheetVersionNumber,
            LastModifiedTime,	
            sheetRowNumber,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
            ROW_NUMBER() OVER (PARTITION BY platform_name, brand_name, year, month order by {{daton_batch_runtime()}} desc) as row_num
            from {{i}}  
             {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                qualify {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}                    
            )
            where row_num =1  
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
