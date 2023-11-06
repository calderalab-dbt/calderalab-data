
{% if var('recharge_flag') %}
-- depends_on: {{ ref('RechargeOrdersLineItemsProperties') }}
{% endif %}

{% if var('upscribe_flag') %}
-- depends_on: {{ ref('UpscribeSubscriptionItems') }}
{% endif %}

select
ord.order_id,
ord.brand,
'Shopify' as platform_name,
{{store_name('store')}},
cast(ord.line_items_product_id as string) as product_id, 
ord.line_items_sku as sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,
{% if var('recharge_flag') %}
  recharge.subscription_id,
{% elif var('upscribe_flag') %}
  upscribe.subscription_id,
{% else %}
  '' as subscription_id,
{% endif %}
'Order' as transaction_type, 
false as is_cancelled,
'' as reason,
sum(line_items_quantity) quantity,
sum(CAST(line_items_price AS numeric)*line_items_quantity + ifnull(CAST(tax_lines_price AS numeric),0)) as total_price,
sum(CAST(line_items_price AS numeric)*line_items_quantity) as subtotal_price,
sum(CAST(tax_lines_price AS numeric)) as total_tax, 
cast(null as numeric) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(presentment_money_amount) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('ShopifyOrdersLineItems') }} ord
left join (select order_id, line_items_product_id, line_items_sku, sum(presentment_money_amount) as presentment_money_amount from {{ ref('ShopifyOrdersDiscountAllocations') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
{% endif %}
group by 1,2,3) disc_alloc
on ord.order_id= disc_alloc.order_id and ord.line_items_sku = disc_alloc.line_items_sku and ord.line_items_product_id = disc_alloc.line_items_product_id

-- fetching the subscription ids in case of recharge orders
{% if var('recharge_flag') %}
  left join (
  select distinct 'Recharge' as order_channel, 
  external_order_id as order_id, 
  sku, 
  case when name ='subscription_id' then value
  when name ='add_on_subscription_id' then value 
  end as subscription_id
  from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
  on ord.order_id = recharge.order_id and ord.line_items_sku = recharge.sku
{% endif %}

-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel, 
  cast(shopify_order_id as string) as order_id,
  items_sku as sku,
  subscription_id 
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ord.order_id= upscribe.order_id and ord.line_items_sku = upscribe.sku
{% endif %}

left join 
(select order_id,line_items_product_id,line_items_sku,sum(CAST(tax_lines_price AS numeric)) tax_lines_price 
from {{ref('ShopifyOrdersLineItemsTaxLines')}} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
{% endif %}
group by 1,2,3) tax
on ord.order_id= tax.order_id and ord.line_items_sku = tax.line_items_sku and ord.line_items_product_id = tax.line_items_product_id
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(ord.updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ord.updated_at)") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,22

UNION ALL

select 
cast(ref.order_id as string) as order_id,
brand,
'Shopify' as platform_name,
{{store_name('store')}},
cast(ref.line_item_product_id as string) as product_id, 
ref.line_item_sku as sku,
subtotal_set_presentment_currency_code as currency,
exchange_currency_code, 
exchange_currency_rate,
date(ref.created_at) as date,
{% if var('recharge_flag') %}
  recharge.subscription_id,
{% elif var('upscribe_flag') %}
  upscribe.subscription_id,
{% else %}
'' as subscription_id,
{% endif %}
'Return' as transaction_type, 
false as is_cancelled,
'' as reason,
sum(line_item_quantity) as quantity,
sum(CAST(line_item_price AS numeric)*line_item_quantity + ifnull(CAST(refund_line_items_total_tax AS numeric),0)) total_price,
sum(CAST(line_item_price AS numeric)*line_item_quantity) subtotal_price,
sum(cast(refund_line_items_total_tax as numeric)) total_tax,
cast(null as numeric) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(presentment_money_amount) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('ShopifyRefundsLineItems')}} ref
left join (select order_id, line_items_product_id, line_items_sku, sum(presentment_money_amount) as presentment_money_amount from {{ ref('ShopifyOrdersDiscountAllocations') }} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(updated_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(updated_at)") }}
{% endif %}
group by 1,2,3) disc_alloc
on cast(ref.order_id as string) = disc_alloc.order_id and cast(ref.line_item_sku as string) = disc_alloc.line_items_sku and ref.line_item_product_id = disc_alloc.line_items_product_id

-- fetching the subscription ids in case of recharge orders
{% if var('recharge_flag') %}
  left join (
  select distinct 'Recharge' as order_channel, 
  external_order_id as order_id, 
  sku, 
  case when name ='subscription_id' then value
  when name ='add_on_subscription_id' then value 
  end as subscription_id
  from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
  on ref.refund_id = recharge.order_id and ref.line_item_sku = recharge.sku
{% endif %}

-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel, 
  cast(shopify_order_id as string) as order_id,
  items_sku as sku,
  subscription_id 
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ref.refund_id= upscribe.order_id and ref.line_item_sku = upscribe.sku
{% endif %} 

left join 
(select cast(refund_id as string) as order_id,line_item_sku as items_sku, sum(cast(tax_lines_price as numeric)) refund_total_tax 
from {{ref('ShopifyRefundLineItemsTax')}} 
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(processed_at)") }}
{% endif %}
group by 1,2) ref_tax
on cast(ref.refund_id as string)= ref_tax.order_id and ref.line_item_sku = ref_tax.items_sku
{% if not flags.FULL_REFRESH %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date(ref.processed_at) >= {{ dbt.dateadd(datepart="day", interval=-7, from_date_or_timestamp="date(ref.processed_at)") }}
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,22

