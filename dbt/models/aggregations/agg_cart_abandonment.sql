-- agg_cart_abandonment.sql
-- Top pickup/dropoff zone pairs by trip count and revenue.

{{ config(materialized='table') }}

select
    pickup_date,
    pulocation_id,
    dolocation_id,
    count(*) as total_trips,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_total_amount,
    avg(trip_duration_min) as avg_trip_duration_min

from {{ ref('fct_event') }}
group by pickup_date, pulocation_id, dolocation_id
order by total_revenue desc
