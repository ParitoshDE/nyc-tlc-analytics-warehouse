-- fct_event.sql
-- Trip fact table at the grain of one trip.

{{ config(
    materialized='table',
    partition_by={
        "field": "pickup_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["pulocation_id", "dolocation_id", "payment_type"]
) }}

with trips as (
    select
        *,
        row_number() over (
            partition by vendor_id, pickup_datetime, pulocation_id, dolocation_id
            order by dropoff_datetime, total_amount, fare_amount, tip_amount, trip_distance
        ) as trip_seq
    from {{ ref('stg_events') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'pulocation_id', 'dolocation_id', 'trip_seq']) }} as event_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    day_of_week,
    passenger_count,
    trip_distance,
    pulocation_id,
    dolocation_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    trip_duration_min

from trips
