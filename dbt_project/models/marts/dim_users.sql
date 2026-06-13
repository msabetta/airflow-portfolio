with stg_users as (
    select * from {{ ref('stg_users') }}
)

select
    user_id,
    first_name,
    last_name,
    email,
    country,
    registration_date
from stg_users
