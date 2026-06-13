with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

stg_products as (
    select * from {{ ref('stg_products') }}
),

stg_users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        o.order_id,
        o.user_id,
        o.product_id,
        o.quantity,
        o.unit_price,
        (o.quantity * o.unit_price) as total_amount,
        o.order_timestamp,
        cast(o.order_timestamp as date) as order_date,
        o.status,
        u.country as user_country,
        p.category as product_category
    from stg_orders o
    left join stg_users u on o.user_id = u.user_id
    left join stg_products p on o.product_id = p.product_id
)

select * from final
