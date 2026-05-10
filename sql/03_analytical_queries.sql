-- =============================================================================
-- Analytical Queries
-- Redshift: star schema joins | Athena: partition-pruned S3 queries
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- REDSHIFT QUERIES (star schema joins)
-- ─────────────────────────────────────────────────────────────────────────────

-- Revenue by product category
SELECT
    dp.product_category,
    COUNT(f.order_id)             AS total_orders,
    SUM(f.quantity)               AS total_units_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_orders f
JOIN dim_product dp ON f.product_id = dp.product_id
GROUP BY dp.product_category
ORDER BY total_revenue DESC;


-- Revenue by customer city
SELECT
    dc.customer_city,
    COUNT(DISTINCT f.customer_id) AS unique_customers,
    COUNT(f.order_id)             AS total_orders,
    ROUND(SUM(f.total_amount), 2) AS city_revenue
FROM fact_orders f
JOIN dim_customer dc ON f.customer_id = dc.customer_id
GROUP BY dc.customer_city
ORDER BY city_revenue DESC;


-- Top customers by lifetime value
SELECT
    dc.customer_name,
    dc.customer_city,
    COUNT(f.order_id)             AS orders_placed,
    ROUND(SUM(f.total_amount), 2) AS lifetime_value
FROM fact_orders f
JOIN dim_customer dc ON f.customer_id = dc.customer_id
GROUP BY dc.customer_name, dc.customer_city
ORDER BY lifetime_value DESC
LIMIT 20;


-- Revenue by product category and city (multi-dimension join)
SELECT
    dp.product_category,
    dc.customer_city,
    SUM(f.total_amount)  AS total_revenue,
    COUNT(f.order_id)    AS order_count
FROM fact_orders f
JOIN dim_customer dc ON f.customer_id = dc.customer_id
JOIN dim_product  dp ON f.product_id  = dp.product_id
GROUP BY dp.product_category, dc.customer_city
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- ATHENA QUERIES (direct S3, partition-pruned)
-- Replace lakehouse_db.orders_curated with your actual catalog table name.
-- Always include WHERE year/month filters — Athena charges $5/TB scanned.
-- ─────────────────────────────────────────────────────────────────────────────

-- Total revenue by product category (full year scan)
SELECT
    product_category,
    SUM(quantity * unit_price) AS total_revenue,
    COUNT(*)                   AS order_count
FROM lakehouse_db.orders_curated
GROUP BY product_category
ORDER BY total_revenue DESC;


-- Single-month partition-pruned query
-- Athena reads ONLY year=2024/month=1/ — skips all other partitions
SELECT
    customer_name,
    product_name,
    total_amount
FROM lakehouse_db.orders_curated
WHERE year = '2024' AND month = '1'
ORDER BY total_amount DESC;


-- Monthly revenue trend (full year, one partition per month)
SELECT
    year,
    month,
    COUNT(*)                    AS order_count,
    ROUND(SUM(total_amount), 2) AS monthly_revenue
FROM lakehouse_db.orders_curated
WHERE year = '2024'
GROUP BY year, month
ORDER BY CAST(month AS INTEGER);


-- Category breakdown for a single month (partition-pruned)
SELECT
    product_category,
    COUNT(order_id)             AS orders,
    SUM(quantity)               AS units_sold,
    ROUND(SUM(total_amount), 2) AS revenue
FROM lakehouse_db.orders_curated
WHERE year = '2024' AND month = '1'
GROUP BY product_category
ORDER BY revenue DESC;


-- Data quality check — run after every ETL to catch issues early
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(order_id)                                 AS non_null_order_ids,
    COUNT(customer_id)                              AS non_null_customer_ids,
    COUNT(CASE WHEN total_amount <= 0 THEN 1 END)   AS zero_or_negative_amounts,
    ROUND(AVG(total_amount), 2)                     AS avg_order_value
FROM lakehouse_db.orders_curated
WHERE year = '2024';
