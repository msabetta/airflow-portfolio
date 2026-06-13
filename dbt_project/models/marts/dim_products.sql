with stg_products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    category,
    price
from stg_products
