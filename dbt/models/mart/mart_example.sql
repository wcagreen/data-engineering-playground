-- models/mart/mart_example.sql
-- ------------------------------
-- Production mart model: final business logic and aggregations.
-- References the staging model so dbt manages the dependency order.
-- This model materialises as a TABLE in the production schema.
--
-- Rename this file, e.g.  mart_orders_daily.sql

{{ config(schema='production') }}

SELECT
    -- ── Business logic & aggregations go here ─────────────────────────────
    -- Example (daily order summary):
    --   order_date,
    --   COUNT(*)            AS order_count,
    --   SUM(amount)         AS total_revenue,
    --   AVG(amount)         AS avg_order_value
    -- GROUP BY order_date
    *

FROM {{ ref('stg_example') }}   -- ← update to your staging model name
