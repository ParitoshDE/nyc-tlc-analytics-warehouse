-- dim_session.sql
-- Payment type dimension with trip and revenue stats.

{{ config(materialized='table') }}

select
    payment_type,
    count(*)               as total_trips,
    sum(total_amount)      as total_revenue,
    avg(total_amount)      as avg_total_amount,
    avg(tip_amount)        as avg_tip_amount,
    avg(trip_duration_min) as avg_trip_duration_min

from {{ ref('stg_events') }}
where payment_type is not null

group by payment_type
