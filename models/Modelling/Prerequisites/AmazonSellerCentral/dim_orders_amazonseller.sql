{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Seller Central' as platform_name,
    amazon_order_id as order_id,
    coalesce(BuyerInfo_BuyerEmail,'') as customer_id, 
    cast(null as string) as payment_mode,
    'Online Marketplace' as order_channel,
    rating,
    comments,
    date(last_updated_date) as last_updated_date,
    row_number() over(partition by amazon_order_id order by last_updated_date desc) row_num
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
    left join (select distinct amazonorderid, BuyerInfo_BuyerEmail from {{ ref('ListOrder') }}) lst_ord
    on ord.amazon_order_id = lst_ord.amazonorderid
    left join (select distinct order_id, rating, comments from {{ ref('FlatFileFeedbackReports') }}) fbk
    on ord.amazon_order_id = fbk.order_id
    {% if not flags.FULL_REFRESH %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE date(ord.last_updated_date) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ord.last_updated_date)") }}
    {% endif %}
    )
where row_num = 1