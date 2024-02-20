{% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

select *
from (
select
coalesce(BuyerInfo_BuyerEmail,'') as customer_id, 
coalesce(BuyerInfo_BuyerEmail,'') as email, 
'Amazon Seller Central' as acquisition_channel,
date(PurchaseDate) as order_date,
-- amazonorderid as order_id,
date(LastUpdateDate) as last_updated_date,
row_number() over(partition by BuyerInfo_BuyerEmail order by date(PurchaseDate) asc) as row_num
from {{ ref('ListOrder') }}
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(LastUpdateDate) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(LastUpdateDate)") }}
{% endif %}
) x 
where row_num=1