-- =============================================================================
-- Load data from S3 into Redshift Serverless
-- Replace placeholders before running:
--   <BUCKET_NAME>  — your S3 bucket name
--   <ACCOUNT_ID>   — your 12-digit AWS account ID
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 1: Load dimension tables from raw CSV
-- Dimensions must be loaded before the fact table (FK constraints)
-- -----------------------------------------------------------------------------

COPY dim_customer (customer_id, customer_name, customer_city)
FROM 's3://<BUCKET_NAME>/raw/dimensions/customers.csv'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3ReadRole'
FORMAT AS CSV
IGNOREHEADER 1;

COPY dim_product (product_id, product_name, product_category)
FROM 's3://<BUCKET_NAME>/raw/dimensions/products.csv'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3ReadRole'
FORMAT AS CSV
IGNOREHEADER 1;

-- -----------------------------------------------------------------------------
-- Step 2: Load fact table from curated Parquet
-- The curated zone holds Glue ETL output — cast types and derived columns
-- are already applied (total_amount, year, month).
-- -----------------------------------------------------------------------------

COPY fact_orders (order_id, order_date, customer_id, product_id, quantity, unit_price, total_amount)
FROM 's3://<BUCKET_NAME>/curated/orders/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3ReadRole'
FORMAT AS PARQUET;

-- -----------------------------------------------------------------------------
-- Step 3: Verify row counts immediately after every COPY.
-- A successful COPY with 0 rows is NOT an error in Redshift — it means
-- no files matched the path or types didn't align. Always check.
-- -----------------------------------------------------------------------------

SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dim_customer
UNION ALL
SELECT 'dim_product',                COUNT(*)               FROM dim_product
UNION ALL
SELECT 'fact_orders',                COUNT(*)               FROM fact_orders;
