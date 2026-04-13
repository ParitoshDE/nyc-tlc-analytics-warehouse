-- fct_event.sql
-- Event fact table at the grain of one event.

{{ config(
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["event_type", "category_level1"]
) }}

select
    {{ dbt_utils.generate_surrogate_key(['user_session', 'event_time', 'product_id', 'event_type']) }} as event_id,
    event_time,
    event_date,
    hour,
    day_of_week,
    event_type,
    product_id,
    category_level1,
    category_level2,
    brand,
    price,
    user_id,
    user_session

from {{ ref('stg_events') }}
