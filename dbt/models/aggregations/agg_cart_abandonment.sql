-- agg_cart_abandonment.sql
-- Cart abandonment: sessions where items were carted but never purchased.

{{ config(materialized='table') }}

with abandoned_sessions as (

    select
        s.user_session,
        s.user_id,
        s.session_date,
        s.duration_sec,
        s.event_count,
        s.session_revenue

    from {{ ref('dim_session') }} s
    where s.has_cart = 1
      and s.has_purchase = 0

),

abandoned_products as (

    select
        a.user_session,
        a.session_date,
        e.category_level1,
        e.brand,
        e.price

    from abandoned_sessions a
    inner join {{ ref('fct_event') }} e
        on a.user_session = e.user_session
    where e.event_type = 'cart'

)

select
    session_date,
    category_level1,
    brand,
    count(distinct user_session) as abandoned_sessions,
    count(*)                     as abandoned_items,
    sum(price)                   as abandoned_value,
    avg(price)                   as avg_abandoned_price

from abandoned_products
group by session_date, category_level1, brand
order by abandoned_value desc
