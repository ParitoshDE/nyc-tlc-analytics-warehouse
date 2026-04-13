-- dim_product.sql
-- Zone dimension from pickup/dropoff location IDs.

{{ config(materialized='table') }}

with all_zones as (

    select
        pulocation_id as location_id,
        'pickup' as zone_role
    from {{ ref('stg_events') }}
    where pulocation_id is not null

    union all

    select
        dolocation_id as location_id,
        'dropoff' as zone_role
    from {{ ref('stg_events') }}
    where dolocation_id is not null

)

select
    location_id,
    count(*) as total_trips_touching_zone,
    countif(zone_role = 'pickup') as pickup_trips,
    countif(zone_role = 'dropoff') as dropoff_trips

from all_zones
group by location_id
