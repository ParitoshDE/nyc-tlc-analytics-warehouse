-- dim_user.sql
-- One row per user with first/last seen and total sessions.

{{ config(materialized='table') }}

select
    user_id,
    min(event_date)                   as first_seen_date,
    max(event_date)                   as last_seen_date,
    count(distinct user_session)      as total_sessions,
    count(*)                          as total_events,
    countif(event_type = 'purchase')  as total_purchases

from {{ ref('stg_events') }}

group by user_id
