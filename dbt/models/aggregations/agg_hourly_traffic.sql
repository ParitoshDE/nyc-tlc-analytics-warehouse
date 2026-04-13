-- agg_hourly_traffic.sql
-- Hourly traffic heatmap: event counts and conversion rates by hour × day_of_week.

{{ config(materialized='table') }}

select
    hour,
    day_of_week,
    count(*)                                                       as total_events,
    countif(event_type = 'view')                                   as views,
    countif(event_type = 'cart')                                   as carts,
    countif(event_type = 'purchase')                               as purchases,
    safe_divide(countif(event_type = 'purchase'), countif(event_type = 'view')) as view_to_purchase_rate,
    sum(case when event_type = 'purchase' then price else 0 end)   as revenue

from {{ ref('fct_event') }}
group by hour, day_of_week
order by day_of_week, hour
