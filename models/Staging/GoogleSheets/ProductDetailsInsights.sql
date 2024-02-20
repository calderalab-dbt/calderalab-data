{% if var('product_details_gs_flag',True) %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}    
    
    
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'start_date', 'data_type': 'date' },
    cluster_by = ['start_date','sku'], 
    unique_key = ['platform_name','sku','start_date','end_date'])}}



{% set table_name_query %}
{{set_table_name('%productdetails%product%')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% for i in results_list %}
    SELECT * 
    /* {{exclude()}} (row_num) */
    From (
        select 
        'Amazon' as platform_name,		
        SKU_Amazon as sku,			
        product_name as description,	
        category,	
        sub_category,		
        cast(null as numeric) as mrp,	
        cogs,
        currency_code,	
        date(start_date) as start_date,
        date(end_date) as end_date,	
        cast(null as string) as sheetVersionNumber,	
        cast(null as timestamp) as LastModifiedTime,	
        cast(null as int) as sheetRowNumber,
        sku1,
        sku2,
        sku3,
        sku4,
        sku5,
        label1,
        label2,
        label3,
        label4,
        label5,
        /* {{daton_user_id()}},
        {{daton_batch_runtime()}},
        {{daton_batch_id()}},
         ROW_NUMBER() OVER (PARTITION BY SKU_Amazon, start_date, end_date  order by {{daton_batch_runtime()}} desc) as row_num */
        from {{i}}  where SKU_Amazon is not null and length(trim(SKU_Amazon))!=0
           /* {% if is_incremental() %} */
            {# /* -- this filter will only be applied on an incremental run */ #}
            /*qualify {{daton_batch_runtime()}}  >= {{max_loaded}}*/
         /*   {% endif %}     */   
                    
        )
        -- where row_num =1  

        union all

        SELECT * 
        /* {{exclude()}} (row_num) */
        From (
        select 
        'Shopify' as platform_name,		
        SKU_Shopify as sku,			
        product_name as description,	
        category,	
        sub_category,		
        cast(null as numeric) as mrp,	
        cogs,
        currency_code,	
        date(start_date) as start_date,
        date(end_date) as end_date,		
        cast(null as string) as sheetVersionNumber,	
        cast(null as timestamp) as LastModifiedTime,	
        cast(null as int) as sheetRowNumber,
        sku1,
        sku2,
        sku3,
        sku4,
        sku5,
        label1,
        label2,
        label3,
        label4,
        label5,
        /* {{daton_user_id()}},
        {{daton_batch_runtime()}},
        {{daton_batch_id()}},
         ROW_NUMBER() OVER (PARTITION BY SKU_Amazon, start_date, end_date  order by {{daton_batch_runtime()}} desc) as row_num */
        from {{i}}  where SKU_Shopify is not null and length(trim(SKU_Shopify))!=0
           /* {% if is_incremental() %} */
            {# /* -- this filter will only be applied on an incremental run */ #}
           /* qualify {{daton_batch_runtime()}}  >= {{max_loaded}}*/
         /*   {% endif %}     */   
                    
        )
        -- where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
