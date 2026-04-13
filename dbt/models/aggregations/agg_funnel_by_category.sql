-- agg_funnel_by_category.sql
-- Conversion funnel: view → cart → purchase rates by product category.

{{ config(materialized='table') }}

with session_categories as (

    select
        s.user_session,
        e.category_level1,
        s.has_view,
        s.has_cart,
        s.has_purchase

    from {{ ref('dim_session') }} s
    inner join {{ ref('fct_event') }} e
        on s.user_session = e.user_session
    group by
        s.user_session,
        e.category_level1,
        s.has_view,
        s.has_cart,
        s.has_purchase

)

select
    category_level1,
    count(distinct user_session)                                           as total_sessions,
    countif(has_view = 1)                                                  as sessions_with_view,
    countif(has_cart = 1)                                                  as sessions_with_cart,
    countif(has_purchase = 1)                                              as sessions_with_purchase,
    safe_divide(countif(has_cart = 1), countif(has_view = 1))              as view_to_cart_rate,
    safe_divide(countif(has_purchase = 1), countif(has_cart = 1))          as cart_to_purchase_rate,
    safe_divide(countif(has_purchase = 1), countif(has_view = 1))          as view_to_purchase_rate

from session_categories
group by category_level1
order by total_sessions desc
