{{ config(
    materialized='table'
)}}

select
acquisition_month,
aquisition_ordertype,
sum(revenue) LTR,
sum(new_customers) new_customers,
sum(revenue) / sum(new_customers) CLTR,
(sum(revenue) / sum(new_customers)) * 0.8 CLTV_80perc,
(sum(revenue) / sum(new_customers)) * 0.6 CLTV_60perc,
FROM {{ ref('customer_cohorts_base')}}
group by 1,2
order by 1 desc,2