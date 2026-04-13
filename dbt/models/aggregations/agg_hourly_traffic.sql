-- agg_hourly_traffic.sql
-- Hourly taxi traffic and revenue by day of week.

{{ config(materialized='table') }}

select
    pickup_hour,
    day_of_week,
    count(*) as total_trips,
    avg(trip_duration_min) as avg_trip_duration_min,
    avg(trip_distance) as avg_trip_distance,
    sum(total_amount) as total_revenue

from {{ ref('fct_event') }}
group by pickup_hour, day_of_week
order by day_of_week, pickup_hour
