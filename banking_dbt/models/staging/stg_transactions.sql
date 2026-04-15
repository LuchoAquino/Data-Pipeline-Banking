{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        v:id::string                 AS transaction_id,
        v:account_id::string         AS account_id,
        v:amount::float              AS amount,
        v:txn_type::string           AS transaction_type,
        v:related_account_id::string AS related_account_id,
        v:status::string             AS status,
        v:created_at::timestamp      AS transaction_time,
        CURRENT_TIMESTAMP            AS load_timestamp
    FROM {{ source('raw', 'transactions') }}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY transaction_time DESC
        ) as rn
    FROM source_data
)

SELECT 
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    transaction_time,
    load_timestamp
FROM deduplicated
WHERE rn = 1