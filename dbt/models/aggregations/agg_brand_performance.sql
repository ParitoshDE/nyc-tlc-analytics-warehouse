-- agg_brand_performance.sql
-- Brand-level revenue, order count, AOV, and conversion rate.

{{ config(materialized='table') }}

with brand_events as (

    select
        brand,
        event_type,
        price,
        user_session

    from {{ ref('fct_event') }}

),

brand_sessions as (

    select
        brand,
        count(distinct user_session)                                      as total_sessions,
        count(distinct case when event_type = 'purchase' then user_session end) as purchase_sessions

    from brand_events
    group by brand

),

brand_revenue as (

    select
        brand,
        count(*)           as purchase_count,
        sum(price)         as total_revenue,
        avg(price)         as avg_order_value

    from brand_events
    where event_type = 'purchase'
    group by brand

)

select
    s.brand,
    s.total_sessions,
    s.purchase_sessions,
    coalesce(r.purchase_count, 0)  as purchase_count,
    coalesce(r.total_revenue, 0)   as total_revenue,
    coalesce(r.avg_order_value, 0) as avg_order_value,
    safe_divide(s.purchase_sessions, s.total_sessions) as conversion_rate

from brand_sessions s
left join brand_revenue r on s.brand = r.brand
order by total_revenue desc
