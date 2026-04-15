-- This is useful for tracking changes in customer information over time. For example, if a customer changes their email address, we want to keep a history of their old email address for reference.
-- Instead of manually writing complex SQL to handle this, the author uses dbt's Snapshots:
-- Configuration: You define a snapshot with a unique key (customer_id) and specify which columns to check for changes (first_name, last_name, email).
-- Strategy "Check": You tell dbt to monitor the staging customer table specifically for changes in the email, first_name, and last_name columns.
-- The Magic of dbt: When you run dbt snapshot, the tool compares the raw table with the previous snapshot.
-- SCD Type 2 (Slowly Changing Dimensions Type 2)

{% snapshot customers_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email']
    )
}}
SELECT * FROM {{ ref('stg_customers') }}
{% endsnapshot %}