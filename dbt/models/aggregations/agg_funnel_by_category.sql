-- agg_funnel_by_category.sql
-- Trip volume and fare metrics by pickup zone.

{{ config(materialized='table') }}

select
    pulocation_id,
    count(*) as total_trips,
    avg(trip_distance) as avg_trip_distance,
    avg(total_amount) as avg_total_amount,
    sum(total_amount) as total_revenue

from {{ ref('fct_event') }}
group by pulocation_id
order by total_trips desc
