with source as (
    select * from {{ source('ecommerce_raw', 'users') }}
),

renamed as (
    select
        user_id,
        first_name,
        last_name,
        email,
        country,
        cast(registration_date as date) as registration_date
    from source
)

select * from renamed
