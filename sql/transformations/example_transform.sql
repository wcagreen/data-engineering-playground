-- sql/transformations/example_transform.sql
-- -------------------------------------------
-- Example SQL transformation to run manually (via psql, pgAdmin, or a
-- PostgresOperator) against data already loaded into staging.
--
-- This demonstrates the "T" step in a pure-SQL ELT approach without dbt.
-- For the dbt-powered approach, see  dbt/models/mart/mart_example.sql

-- ── Example: deduplicate and clean a staging table ───────────────────────────

INSERT INTO production.your_table (
    -- list your target columns here
    id,
    -- customer_name,
    -- order_date,
    -- amount,
    _loaded_at
)
SELECT DISTINCT ON (id)   -- keep first occurrence per id
    id::INTEGER,
    -- TRIM(customer_name)    AS customer_name,
    -- order_date::DATE       AS order_date,
    -- amount::NUMERIC(12, 2) AS amount,
    NOW()                  AS _loaded_at
FROM staging.your_table
WHERE id IS NOT NULL
ORDER BY id
ON CONFLICT (id) DO UPDATE
    SET -- update changed columns on re-run
        -- customer_name = EXCLUDED.customer_name,
        _loaded_at    = EXCLUDED._loaded_at;


-- ── Example: daily aggregation ────────────────────────────────────────────────

-- INSERT INTO production.daily_summary (order_date, order_count, total_revenue)
-- SELECT
--     order_date::DATE       AS order_date,
--     COUNT(*)               AS order_count,
--     SUM(amount)            AS total_revenue
-- FROM staging.your_table
-- WHERE order_date IS NOT NULL
-- GROUP BY order_date::DATE
-- ON CONFLICT (order_date) DO UPDATE
--     SET order_count   = EXCLUDED.order_count,
--         total_revenue = EXCLUDED.total_revenue;
