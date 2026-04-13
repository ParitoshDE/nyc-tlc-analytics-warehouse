-- dim_user.sql
-- Vendor dimension with trip/revenue statistics.

{{ config(materialized='table') }}

select
    vendor_id,
    min(pickup_date)             as first_seen_date,
    max(pickup_date)             as last_seen_date,
    count(*)                     as total_trips,
    sum(total_amount)            as total_revenue,
    avg(total_amount)            as avg_trip_revenue,
    avg(trip_duration_min)       as avg_trip_duration_min

from {{ ref('stg_events') }}
where vendor_id is not null

group by vendor_id
