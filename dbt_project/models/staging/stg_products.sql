with source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),

renamed as (
    select
        product_id,
        product_name,
        category,
        cast(price as float64) as price
    from source
)

select * from renamed
