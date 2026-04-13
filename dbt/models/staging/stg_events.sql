-- stg_events.sql
-- Staging model: clean, typed NYC TLC trips from BigQuery raw layer.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce_raw', 'trips') }}

),

cleaned as (

    select
        cast(vendor_id as int64)            as vendor_id,
        cast(pickup_datetime as timestamp)  as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(pickup_date as date)           as pickup_date,
        cast(pickup_hour as int64)          as pickup_hour,
        cast(day_of_week as int64)          as day_of_week,
        cast(passenger_count as float64)    as passenger_count,
        cast(trip_distance as float64)      as trip_distance,
        cast(pulocation_id as int64)        as pulocation_id,
        cast(dolocation_id as int64)        as dolocation_id,
        cast(payment_type as int64)         as payment_type,
        cast(fare_amount as float64)        as fare_amount,
        cast(tip_amount as float64)         as tip_amount,
        cast(total_amount as float64)       as total_amount,
        cast(trip_duration_min as float64)  as trip_duration_min

    from source

)

select * from cleaned
