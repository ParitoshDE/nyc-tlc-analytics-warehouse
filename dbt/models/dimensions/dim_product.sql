-- dim_product.sql
-- One row per product with latest brand and category info.

{{ config(materialized='table') }}

with products as (

    select
        product_id,
        category_id,
        category_level1,
        category_level2,
        brand,
        row_number() over (
            partition by product_id
            order by event_time desc
        ) as rn

    from {{ ref('stg_events') }}

)

select
    product_id,
    category_id,
    category_level1,
    category_level2,
    brand

from products
where rn = 1
