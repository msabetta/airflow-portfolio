with source as (
    -- In BigQuery, wildcard tables can be queried using `orders_*`
    select * from {{ source('ecommerce_raw', 'orders_*') }}
),

renamed as (
    select
        order_id,
        user_id,
        product_id,
        cast(quantity as int64) as quantity,
        cast(unit_price as float64) as unit_price,
        cast(order_timestamp as timestamp) as order_timestamp,
        status
    from source
)

select * from renamed
