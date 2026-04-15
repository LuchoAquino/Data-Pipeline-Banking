-- Tables that represent entities, such as accounts and customers, are typically modeled as dimension tables.
-- They often contain descriptive attributes (like account_type or customer_name) that provide context for analysis.
-- Using snapshots allows us to track changes in these attributes over time, which is crucial for accurate historical analysis and reporting.
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('accounts_snapshot') }}
)

SELECT * FROM source_data