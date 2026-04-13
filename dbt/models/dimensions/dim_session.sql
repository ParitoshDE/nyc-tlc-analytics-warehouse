-- dim_session.sql
-- One row per session with duration, event counts, and funnel flags.

{{ config(materialized='table') }}

select
    user_session,
    user_id,
    min(event_time)             as session_start,
    max(event_time)             as session_end,
    min(event_date)             as session_date,
    max(session_duration_sec)   as duration_sec,
    max(session_event_count)    as event_count,
    max(session_has_view)       as has_view,
    max(session_has_cart)       as has_cart,
    max(session_has_purchase)   as has_purchase,
    sum(case when event_type = 'purchase' then price else 0 end) as session_revenue

from {{ ref('stg_events') }}

group by user_session, user_id
