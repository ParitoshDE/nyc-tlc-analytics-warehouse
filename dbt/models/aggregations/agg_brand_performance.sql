-- agg_brand_performance.sql
-- Payment type performance: volume, revenue, and tip rate.

{{ config(materialized='table') }}

select
    payment_type,
    count(*) as total_trips,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_total_amount,
    avg(tip_amount) as avg_tip_amount,
    safe_divide(sum(tip_amount), nullif(sum(fare_amount), 0)) as tip_to_fare_ratio

from {{ ref('fct_event') }}
group by payment_type
order by total_revenue desc
