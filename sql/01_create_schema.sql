-- =============================================================================
-- Star Schema — Redshift Serverless
-- Run in order: dimensions first, then fact table (FK constraints)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Dimension tables
-- -----------------------------------------------------------------------------

CREATE TABLE dim_customer (
    customer_id   VARCHAR(10)  PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_city VARCHAR(50)
);

CREATE TABLE dim_product (
    product_id       VARCHAR(10)  PRIMARY KEY,
    product_name     VARCHAR(100),
    product_category VARCHAR(50)
);

-- -----------------------------------------------------------------------------
-- Fact table
-- Stores foreign keys only — never the descriptive text — so a dimension
-- rename (e.g. product_name change) does not silently alter historical records.
-- -----------------------------------------------------------------------------

CREATE TABLE fact_orders (
    order_id      INTEGER       PRIMARY KEY,
    order_date    DATE,
    customer_id   VARCHAR(10)   REFERENCES dim_customer(customer_id),
    product_id    VARCHAR(10)   REFERENCES dim_product(product_id),
    quantity      INTEGER,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2)
);
