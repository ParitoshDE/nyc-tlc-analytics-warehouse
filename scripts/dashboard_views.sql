-- dashboard_views.sql
-- Purpose: create dashboard-friendly semantic views in BigQuery for Looker Studio.
-- Run in BigQuery editor after dbt models are built.

create or replace view `dataengkestra.nyc_tlc_prod.dashboard_kpi_daily` as
select
  pickup_date,
  count(*) as total_trips,
  sum(total_amount) as total_revenue,
  avg(total_amount) as avg_ticket,
  avg(trip_duration_min) as avg_trip_duration_min,
  avg(trip_distance) as avg_trip_distance
from `dataengkestra.nyc_tlc_prod.fct_event`
where pickup_date between date '2023-01-01' and date '2023-12-31'
group by pickup_date
order by pickup_date;

create or replace view `dataengkestra.nyc_tlc_prod.dashboard_hourly_traffic` as
select
  day_of_week,
  case day_of_week
    when 1 then 'Sunday'
    when 2 then 'Monday'
    when 3 then 'Tuesday'
    when 4 then 'Wednesday'
    when 5 then 'Thursday'
    when 6 then 'Friday'
    when 7 then 'Saturday'
    else 'Unknown'
  end as day_name,
  pickup_hour,
  total_trips,
  avg_trip_duration_min,
  avg_trip_distance,
  total_revenue
from `dataengkestra.nyc_tlc_prod.agg_hourly_traffic`;

create or replace view `dataengkestra.nyc_tlc_prod.dashboard_payment_performance` as
select
  payment_type,
  case payment_type
    when 1 then 'Credit card'
    when 2 then 'Cash'
    when 3 then 'No charge'
    when 4 then 'Dispute'
    when 5 then 'Unknown'
    when 6 then 'Voided trip'
    else concat('Type ', cast(payment_type as string))
  end as payment_type_name,
  total_trips,
  total_revenue,
  avg_total_amount,
  avg_tip_amount,
  tip_to_fare_ratio
from `dataengkestra.nyc_tlc_prod.agg_brand_performance`;

create or replace view `dataengkestra.nyc_tlc_prod.dashboard_zone_performance` as
select
  a.pulocation_id,
  p.total_trips_touching_zone,
  p.pickup_trips,
  p.dropoff_trips,
  a.total_trips,
  a.avg_trip_distance,
  a.avg_total_amount,
  a.total_revenue
from `dataengkestra.nyc_tlc_prod.agg_funnel_by_category` a
left join `dataengkestra.nyc_tlc_prod.dim_product` p
  on a.pulocation_id = p.location_id;

create or replace view `dataengkestra.nyc_tlc_prod.dashboard_route_performance` as
select
  pickup_date,
  pulocation_id,
  dolocation_id,
  total_trips,
  total_revenue,
  avg_total_amount,
  avg_trip_duration_min
from `dataengkestra.nyc_tlc_prod.agg_cart_abandonment`;
