 {% if var('AMAZONSELLER') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetailsInsights') }}
{% endif %}

select products.* {{exclude()}}(row_num),
{% if var('product_details_gs_flag') %}
  description, 
  category, 
  sub_category, 
  cast(mrp as numeric) mrp, 
  cast(cogs as numeric) cogs, 
  currency_code,
  cast(start_date as date) start_date, 
  cast(end_date as  date) end_date
{% else %}
  cast(null as string) as description, 
  cast(null as string) as category, 
  cast(null as string) as sub_category, 
  cast(null as numeric) as mrp, 
  cast(null as numeric) as cogs, 
  cast(null as string) as currency_code,
  cast(null as date) as start_date, 
  cast(null as date) as end_date 
{% endif %} 

from (
  select 
  distinct 
  'Amazon Seller Central' as platform_name,
  asin1 as product_id,
  seller_sku as sku,
  summaries_itemName as product_name, 
  summaries_colorName as color,
  summaries_manufacturer as seller,
  summaries_sizeName as size,
  displayGroupRanks_title as product_category,
  image_url,
  status,
  1 as row_num,
  _daton_batch_runtime
  from (select asin1, seller_sku, image_url, status, _daton_batch_runtime from {{ ref('AllListingsReport') }}
  qualify dense_rank() over(order by _daton_batch_runtime desc)=1) alllistings
  left join (select ReferenceASIN, summaries_itemName, summaries_colorName, summaries_manufacturer, summaries_sizeName, displayGroupRanks_title from {{ ref('CatalogItems') }}) catalogitems
  on alllistings.asin1 = catalogitems.ReferenceASIN

  union all

  select 
  distinct 
  'Amazon Seller Central' as platform_name,
  asin as product_id,
  sku as sku,
  summaries_itemName as product_name, 
  summaries_colorName as color,
  summaries_manufacturer as seller,
  summaries_sizeName as size,
  displayGroupRanks_title as product_category,
  cast(null as string) as image_url,
  status,
  2 as row_num,
  _daton_batch_runtime
  from 
  (select asin, sku, status, _daton_batch_runtime from {{ ref('SuppressedListingsReport') }}
  qualify dense_rank() over(order by _daton_batch_runtime desc)=1) suplistings
  left join (select ReferenceASIN, summaries_itemName, summaries_colorName, summaries_manufacturer, summaries_sizeName, displayGroupRanks_title from {{ ref('CatalogItems') }}) catalogitems
  on suplistings.asin = catalogitems.ReferenceASIN

  union all

  select 
  distinct 
  'Amazon Seller Central' as platform_name,
  childASIN as product_id,
  cast(null as string) sku,
  item_name as product_name, 
  cast(null as string) color,
  cast(null as string) seller,
  cast(null as string) size,
  cast(null as string) product_category,
  cast(null as string) image_url,
  cast(null as string) as status,
  3 as row_num,
  _daton_batch_runtime
  from {{ ref('SalesAndTrafficReportByChildASIN') }} traffic
  left join (select distinct asin1, item_name from {{ ref('AllListingsReport') }}) alllistings
  on traffic.childASIN = alllistings.asin1

  union all

  select 
  distinct 
  'Amazon Seller Central' as platform_name,
  asin as product_id,
  cast(sku as string) sku,
  product_name, 
  cast(null as string) color,
  cast(null as string) seller,
  cast(null as string) size,
  cast(null as string) product_category,
  cast(null as string) image_url,
  cast(null as string) as status,
  4 as row_num,
  _daton_batch_runtime
  from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} traffic

) products

{% if var('product_details_gs_flag') %}
left join (
  select 
  sku, 
  description,	
  category, 
  sub_category, 
  mrp, 
  cogs,
  currency_code, 
  start_date, 
  end_date,
  _daton_batch_runtime 
  from {{ ref('ProductDetailsInsights') }} 
  where lower(platform_name) = 'amazon') prod_gs
on products.sku = prod_gs.sku
{% endif %}

qualify row_number() over(partition by product_id, sku order by row_num asc)=1

