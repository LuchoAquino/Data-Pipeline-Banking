{{ config(materialized='view') }} -- Don't store data, always query the original table. Cheap, but slower if it grows

with ranked as (
    select
    -- Flattens the JSON structure
        v:id::string            as account_id,
        v:customer_id::string   as customer_id,
        v:account_type::string  as account_type,
        v:balance::float        as balance,
        v:currency::string      as currency,
        v:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,
    
    -- Selects the latest record for each account_id
    -- based on created_at timestamp to handle updates (if any)
        row_number() over (
            partition by v:id::string
            order by v:created_at desc
        ) as rn
    -- Reads from the raw accounts table
    from {{ source('raw', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1