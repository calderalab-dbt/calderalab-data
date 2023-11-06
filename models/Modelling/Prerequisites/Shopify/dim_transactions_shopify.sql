select
'Shopify' as platform_name,
order_id,
id as transaction_id,
kind as transaction_stage,
gateway as payment_gateway,
message,
cast(null as string) as payment_mode,
status as payment_status,
date(processed_at) as last_updated_date
from {{ ref('ShopifyTransactions') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}