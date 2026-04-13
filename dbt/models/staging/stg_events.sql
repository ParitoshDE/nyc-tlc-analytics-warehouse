-- stg_events.sql
-- Staging model: clean, typed events from BigQuery raw layer.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce_raw', 'events') }}

),

cleaned as (

    select
        cast(event_time as timestamp)       as event_time,
        cast(event_date as date)            as event_date,
        cast(hour as int64)                 as hour,
        cast(day_of_week as int64)          as day_of_week,
        cast(event_type as string)          as event_type,
        cast(product_id as int64)           as product_id,
        cast(category_id as int64)          as category_id,
        coalesce(cast(category_code as string), 'unknown')   as category_code,
        coalesce(cast(category_level1 as string), 'unknown') as category_level1,
        cast(category_level2 as string)     as category_level2,
        coalesce(cast(brand as string), 'unknown')           as brand,
        cast(price as float64)              as price,
        cast(user_id as int64)              as user_id,
        cast(user_session as string)        as user_session,
        cast(session_duration_sec as int64) as session_duration_sec,
        cast(session_event_count as int64)  as session_event_count,
        cast(session_has_purchase as int64) as session_has_purchase,
        cast(session_has_cart as int64)     as session_has_cart,
        cast(session_has_view as int64)     as session_has_view

    from source

)

select * from cleaned
