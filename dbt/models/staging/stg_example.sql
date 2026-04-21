-- models/staging/stg_example.sql
-- --------------------------------
-- Staging model: clean and type-cast raw data from the staging schema.
--
-- Rename this file to match your table, e.g.  stg_orders.sql
-- Update the source() reference to match sources.yml.
-- This model materialises as a VIEW in the staging schema.

{{ config(schema='staging') }}

SELECT
    -- ── Type casts & renames go here ──────────────────────────────────────
    -- Example:
    --   id::INTEGER            AS id,
    --   TRIM(customer_name)    AS customer_name,
    --   order_date::DATE       AS order_date,
    --   amount::NUMERIC(12, 2) AS amount,
    *

FROM {{ source('staging', 'your_table') }}   -- ← update 'your_table'

WHERE 1 = 1
    -- Add row filters here, e.g.
    -- AND status IS NOT NULL
