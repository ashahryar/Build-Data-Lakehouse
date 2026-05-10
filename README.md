# Governed Data Lakehouse on AWS

A complete, production-pattern data lakehouse built on AWS — from raw CSV ingestion to governed, queryable analytics. The system is designed so a business analyst can trust what they're querying, an engineer can trace exactly how data was transformed, and access is controlled by default.

**Services:** Amazon S3 · AWS Glue · Amazon Redshift Serverless · Amazon Athena · Amazon DataZone · AWS Lambda · AWS IAM

---

## Table of Contents

- [Architecture](#architecture)
- [Data Lake Design](#data-lake-design)
- [IAM and Security](#iam-and-security)
- [Schema Discovery with Glue Crawler](#schema-discovery-with-glue-crawler)
- [ETL Transformation with PySpark](#etl-transformation-with-pyspark)
- [Star Schema in Redshift Serverless](#star-schema-in-redshift-serverless)
- [Ad-hoc Querying with Athena](#ad-hoc-querying-with-athena)
- [Data Governance with DataZone](#data-governance-with-datazone)
- [Event-Driven Automation with Lambda](#event-driven-automation-with-lambda)
- [Query Reference](#query-reference)
- [Debugging Log](#debugging-log)
- [Cost Model](#cost-model)
- [What's Next](#whats-next)

---

## Architecture

```
                        ┌─────────────────────────────────────┐
                        │           Amazon S3 (3-Zone Lake)    │
                        │  raw/  →  curated/  →  consumption/ │
                        └────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │              AWS Glue                    │
                    │   Crawler (schema) + ETL Job (PySpark)   │
                    └───────────┬─────────────────────────────┘
                                │
               ┌────────────────▼────────────────┐
               │         Glue Data Catalog        │
               │    (central metadata registry)   │
               └──────────┬──────────────────────┘
                           │
          ┌────────────────┼──────────────────┐
          │                │                  │
          ▼                ▼                  ▼
  Redshift Serverless   Amazon Athena    Amazon DataZone
  (Star Schema DWH)   (S3-direct SQL)   (Governance Layer)
                                              │
                                    ┌─────────▼──────────┐
                                    │    AWS Lambda       │
                                    │  (S3 event trigger) │
                                    └────────────────────┘
```

The system follows the **lakehouse pattern**: a single copy of curated Parquet data in S3 is read by both Redshift (structured warehouse) and Athena (ad-hoc SQL) simultaneously — no duplication, no sync pipeline between them. DataZone governs what gets published, who can access it, and how business terms are defined.

---

## Data Lake Design

```
s3://my-lakehouse-bucket/
├── raw/
│   └── orders/
│       └── orders.csv          ← Source data, never modified after landing
├── curated/
│   └── orders/
│       └── year=2024/
│           ├── month=1/        ← Parquet, partitioned by year and month
│           ├── month=2/
│           └── month=3/
└── consumption/
    └── ...                     ← Pre-aggregated outputs for reporting tools
```

The bucket is split into three zones with distinct responsibilities:

- **raw/** — immutable landing zone. Files are written once and never touched again. This preserves the ability to replay the entire pipeline from scratch if transformation logic changes, since the original source data is always intact.
- **curated/** — transformation output in Parquet format, partitioned by `year` and `month`. Parquet is columnar (Athena reads only the columns a query needs), compresses significantly better than CSV, and embeds schema metadata so downstream tools don't need to infer types. Partitioning means a query filtered to one month reads only that month's folder.
- **consumption/** — downstream-specific outputs. Pre-aggregated monthly summaries or shaped exports live here, computed once and queried repeatedly rather than recomputed on every dashboard load.

---

## IAM and Security

The Glue ETL job runs under a dedicated IAM role with two policy attachments — their separation is intentional:

- **Managed policy** (`AWSGlueServiceRole`) — covers the service-level baseline: CloudWatch logging, Data Catalog access, and Glue service interactions. AWS-managed, applies to any Glue job.
- **Inline policy** — hand-written, scoped only to this lakehouse bucket. Grants `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` on `s3://my-lakehouse-bucket/*` and nothing else. If the role were compromised, the blast radius is limited to this single bucket.

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::my-lakehouse-bucket",
    "arn:aws:s3:::my-lakehouse-bucket/*"
  ]
}
```

> **Common mistake:** Mismatching the role ARN in job configurations with the actual role name in IAM produces silent S3 access denied errors that point to the wrong place. The role name must be identical everywhere it appears — Glue job definition, Redshift COPY command, Lambda execution role.

---

## Schema Discovery with Glue Crawler

Rather than hardcoding schema into the ETL job, the Glue Crawler reads the raw S3 path, samples the files, infers column names and types, and writes the table definition into the Glue Data Catalog. From that point, any AWS service with the right permissions can query the catalog by table name without additional configuration.

**Schema discovered from the retail orders CSV:**

| Column | Inferred Type | Notes |
|---|---|---|
| order_id | bigint | Primary identifier |
| customer_id | string | |
| customer_name | string | |
| customer_city | string | |
| product_id | string | |
| product_name | string | |
| product_category | string | |
| order_date | string | Cast to date in ETL |
| quantity | bigint | Cast to integer in ETL |
| unit_price | double | Correct as-is |

Two columns need correction downstream:
- `order_date` is inferred as `string` because the crawler reads CSVs conservatively — it doesn't know the date format without extra config.
- `quantity` is `bigint` when `integer` is semantically more accurate.

Both are corrected explicitly in the ETL job rather than relying on implicit coercion.

The Glue Data Catalog functions as the central metadata registry. Once a table is registered, Athena, Redshift Spectrum, and EMR can all discover and query it without per-service configuration.

---

## ETL Transformation with PySpark

The Glue ETL job reads raw CSV via the Data Catalog (no hardcoded schema in the script), applies transformations, and writes partitioned Parquet to the curated zone on a Glue-managed PySpark cluster — no infrastructure to provision.

**Transformations applied, in order:**

1. Read from the Glue Data Catalog — schema changes picked up automatically on re-crawl
2. Cast `order_date` string → date
3. Cast `quantity` bigint → integer
4. Cast `unit_price` to double (explicit, even though already correct)
5. Derive `total_amount` = `quantity × unit_price` — pre-computed to avoid redundant multiplication in every downstream query
6. Extract `year` from `order_date` using Spark's `year()` function
7. Extract `month` from `order_date` using Spark's `month()` function
8. Write as Parquet to curated zone, partitioned by `year` and `month`
9. Update the Glue Data Catalog with the curated table definition

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_df = glueContext.create_dynamic_frame.from_catalog(
    database="lakehouse_db",
    table_name="raw_orders"
).toDF()

transformed_df = (
    raw_df
    .withColumn("order_date",    col("order_date").cast("date"))
    .withColumn("quantity",      col("quantity").cast("integer"))
    .withColumn("unit_price",    col("unit_price").cast("double"))
    .withColumn("total_amount",  col("quantity") * col("unit_price"))
    .withColumn("year",          year(col("order_date")).cast("string"))
    .withColumn("month",         month(col("order_date")).cast("string"))
)

transformed_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://my-lakehouse-bucket/curated/orders/")

job.commit()
```

**Why Parquet with partitions:**
- Athena charges $5/TB scanned. Without partitions, a query for one month reads the entire dataset. With `year=2024/month=1/` partitioning, Athena reads only that folder and skips everything else — cost scales linearly with data eliminated by the filter.
- Parquet stores per-column min/max statistics per row group, enabling predicate pushdown inside a partition — reducing scans further when column filters are applied.
- On 36 months of data, a single-month filter reduces the scan to roughly 2.8% of the full table.

---

## Star Schema in Redshift Serverless

Redshift Serverless provides a fully managed warehouse with no cluster to size or maintain. It bills per RPU-second only when queries are running, making it cost-effective for workloads that aren't active 24/7.

### Schema Design

The warehouse uses a classic star schema:

- **Fact table** (`fact_orders`) — measurable events: order IDs, quantities, prices, and revenue. Stores foreign keys to dimensions, not the descriptive text itself.
- **Dimension tables** (`dim_customer`, `dim_product`) — descriptive context: names, cities, categories.

```sql
CREATE TABLE dim_customer (
    customer_id   VARCHAR(50)  PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_city VARCHAR(100)
);

CREATE TABLE dim_product (
    product_id       VARCHAR(50)  PRIMARY KEY,
    product_name     VARCHAR(255) NOT NULL,
    product_category VARCHAR(100)
);

CREATE TABLE fact_orders (
    order_id      BIGINT           PRIMARY KEY,
    customer_id   VARCHAR(50)      REFERENCES dim_customer(customer_id),
    product_id    VARCHAR(50)      REFERENCES dim_product(product_id),
    order_date    DATE,
    quantity      INTEGER,
    unit_price    DOUBLE PRECISION,
    total_amount  DOUBLE PRECISION
);
```

The key design decision: the fact table stores only `product_id`, not `product_name`. When a product is renamed, only `dim_product` is updated — historical order records remain unchanged. Storing the name directly in the fact table would retroactively corrupt historical data on every rename.

### Loading Data

Redshift's `COPY` command parallelizes reads across compute nodes — far faster than row-by-row inserts for bulk loads.

```sql
-- Dimensions first — fact table has FK constraints
COPY dim_customer (customer_id, customer_name, customer_city)
FROM 's3://my-lakehouse-bucket/curated/orders/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftS3Role'
FORMAT AS PARQUET;

COPY dim_product (product_id, product_name, product_category)
FROM 's3://my-lakehouse-bucket/curated/orders/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftS3Role'
FORMAT AS PARQUET;

COPY fact_orders
FROM 's3://my-lakehouse-bucket/curated/orders/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftS3Role'
FORMAT AS PARQUET;
```

> **Silent failure risk:** A successful COPY with 0 rows is not an error in Redshift's view — it just means no files matched the path. Always verify immediately: `SELECT COUNT(*) FROM fact_orders;`. If 0 rows, check the S3 path character by character before investigating anything else.

**Handling Parquet type mismatches:** Spark writes `INT64` for `LongType` columns, which Redshift sometimes misaligns against `INTEGER`. The safest approach is a staging table: load Parquet into a staging table with permissive `VARCHAR` columns, then insert into the final table with explicit `CAST()` per column. This makes mismatches visible rather than silent.

---

## Ad-hoc Querying with Athena

Athena queries S3 directly — no cluster, no data loading, no ETL step. Point it at the Glue Data Catalog table, write SQL, and it reads the Parquet files and returns results. Billing is strictly per TB scanned.

| Engine | Best for |
|---|---|
| Athena | One-off questions, exploration, validation, queries that don't run on a schedule |
| Redshift | Production dashboards, repeated queries needing consistent sub-second response |

Both engines read the same Parquet files — this is the lakehouse pattern. There's no sync pipeline because there's no copy to keep in sync.

**Setup requirements:**
- Create an S3 results bucket: `s3://my-athena-results/query-results/`
- Configure it in Athena workgroup settings before running any query
- Use the **SQL engine workgroup**, not the Spark engine — the Spark workgroup is for PySpark notebooks and rejects standard SQL

**Partition pruning in practice:**

```sql
-- Without filter — scans ALL partitions (entire dataset)
SELECT SUM(total_amount) FROM curated_orders;

-- With filter — scans ONLY year=2024/month=1/, skips everything else
SELECT SUM(total_amount) FROM curated_orders
WHERE year = '2024' AND month = '1';
```

Partition columns must appear in every `WHERE` clause against partitioned tables. Omitting them degrades both cost and query performance — there's no upside to skipping them.

---

## Data Governance with DataZone

Without governance, the data lake has no formal ownership, no standardized definitions, and no controlled access — anyone with IAM access can query anything, and different teams computing the same metric can reach different answers. DataZone addresses all three.

### Structure

DataZone is organized in three layers:

- **Domain** — top-level organizational boundary, typically a business unit or platform team. Defines the governance scope.
- **Projects** — group related assets within a domain, define team boundaries, control who can publish or subscribe.
- **Environments** — connect projects to the underlying AWS infrastructure (Glue databases, Redshift namespaces, S3 paths).

### Asset Publishing and Access Control

Once Glue Data Catalog tables are connected to a DataZone environment, they can be published to the governed catalog:
- Assets become discoverable to other teams
- Access is gated behind a request-and-approve workflow — no direct IAM manipulation required
- A consumer submits a request, the data owner approves it, DataZone provisions the permissions automatically

### Business Glossary

The glossary enforces semantic consistency. Without it, `total_amount` in `fact_orders` means different things to different teams (revenue, shipment value, pre-discount price) and everyone gets different answers from the same query.

| Term | Definition |
|---|---|
| Total Revenue | Sum of `quantity × unit_price` across all completed orders in the selected period, before discounts or returns |
| Customer Lifetime Value | Total revenue attributed to a single `customer_id` from first order to most recent date in the dataset |
| Order | A single transaction record in `fact_orders` linking one customer to one product with a specific quantity and unit price |

Definitions are colocated with the asset in the catalog — not in a Confluence page that may be stale. When an analyst browses the catalog and finds `fact_orders`, they see exactly how each term is calculated.

---

## Event-Driven Automation with Lambda

Without automation, each new file in S3 requires a manual ETL trigger. Lambda eliminates this by wiring an S3 event notification to a function that fires the Glue job automatically — latency from file landing to job start is measured in seconds.

**How it works:**
1. A CSV is uploaded to `s3://my-lakehouse-bucket/raw/orders/`
2. S3 fires a `PutObject` event notification to Lambda
3. Lambda extracts the bucket name and object key from the event payload
4. Validates the key starts with `raw/orders/` — ignores unrelated uploads
5. Calls `glue:StartJobRun` via Boto3

```python
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']

    if not key.startswith('raw/orders/'):
        return {'statusCode': 200, 'body': 'Not a target path, skipping.'}

    response = glue.start_job_run(
        JobName='lakehouse-etl-job',
        Arguments={
            '--source_bucket': bucket,
            '--source_key':    key
        }
    )

    return {
        'statusCode': 200,
        'body': f"Started Glue job run: {response['JobRunId']}"
    }
```

The Lambda execution role needs only `glue:StartJobRun` on the target job ARN.

### Event-Driven vs. Scheduled

| | Event-driven (Lambda) | Scheduled (MWAA / CloudWatch) |
|---|---|---|
| Trigger | Fires on S3 upload | Fires at fixed interval |
| Latency | Under 30 seconds | Up to 59 minutes (hourly schedule) |
| Cost | Free up to 1M invocations/month | ~$350/month just to keep MWAA active |
| Wasted runs | None — only fires when data arrives | Fires 24× daily regardless of data volume |

**Idempotency caveat:** If the same file is uploaded twice — common in production due to upstream retry logic — the ETL job runs twice against the same input. Handle this with Glue job bookmarks (skip already-processed files) or `overwrite` mode (second run replaces first run's output without creating duplicates).

---

## Query Reference

### Redshift — Revenue by Product Category

```sql
SELECT
    p.product_category,
    COUNT(f.order_id)             AS total_orders,
    SUM(f.quantity)               AS total_units_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_orders f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category
ORDER BY total_revenue DESC;
```

### Redshift — Revenue by Customer City

```sql
SELECT
    c.customer_city,
    COUNT(DISTINCT f.customer_id) AS unique_customers,
    COUNT(f.order_id)             AS total_orders,
    ROUND(SUM(f.total_amount), 2) AS city_revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_city
ORDER BY city_revenue DESC;
```

### Redshift — Top Customers by Lifetime Value

```sql
SELECT
    c.customer_name,
    c.customer_city,
    COUNT(f.order_id)             AS orders_placed,
    ROUND(SUM(f.total_amount), 2) AS lifetime_value
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name, c.customer_city
ORDER BY lifetime_value DESC
LIMIT 20;
```

### Athena — Monthly Revenue Rollup (Partition-Pruned)

```sql
SELECT
    year,
    month,
    COUNT(*)                    AS order_count,
    ROUND(SUM(total_amount), 2) AS monthly_revenue
FROM curated_orders
WHERE year = '2024'
GROUP BY year, month
ORDER BY CAST(month AS INTEGER);
```

### Athena — Category Breakdown for a Single Month

```sql
SELECT
    product_category,
    COUNT(order_id)             AS orders,
    SUM(quantity)               AS units_sold,
    ROUND(SUM(total_amount), 2) AS revenue
FROM curated_orders
WHERE year = '2024' AND month = '1'
GROUP BY product_category
ORDER BY revenue DESC;
```

### Athena — Data Quality Check

```sql
SELECT
    COUNT(*)                                      AS total_rows,
    COUNT(order_id)                               AS non_null_order_ids,
    COUNT(customer_id)                            AS non_null_customer_ids,
    COUNT(CASE WHEN total_amount <= 0 THEN 1 END) AS zero_or_negative_amounts,
    ROUND(AVG(total_amount), 2)                   AS avg_order_value
FROM curated_orders
WHERE year = '2024';
```

---

## Debugging Log

Issues encountered during the build, root causes, and resolutions — the kind of bugs that don't appear in documentation.

**Redshift VPC subnet error**
- **Cause:** Redshift Serverless requires subnets in at least 3 different Availability Zones. The VPC only had 2.
- **Fix:** Created 2 additional subnets in separate AZs before retrying namespace creation.
- **Prevention:** Verify AZ coverage before starting the Redshift Serverless setup wizard.

**COPY loaded 0 rows with no error**
- **Cause:** Trailing space in the S3 path — `"curated /"` instead of `"curated/"`. Redshift found no matching objects and silently loaded nothing.
- **Fix:** Removed the extra space.
- **Prevention:** Always run `SELECT COUNT(*) FROM table_name` immediately after every COPY. A 0-row result is the only signal of this failure mode.

**Parquet schema mismatch with Redshift**
- **Cause:** Spark writes `INT64` for `LongType` columns. Redshift's COPY misaligned this against `INTEGER` declarations in some cases.
- **Fix:** Staging table approach — load Parquet into a staging table with all-`VARCHAR` columns, then insert into the final table with explicit `CAST()` per column.
- **Prevention:** Decouple the load step from type enforcement. Mismatches become visible errors rather than silent corruption.

**IAM access denied on Glue job**
- **Cause:** Role ARN in the Glue job config used a hyphen (`glue-lakehouse-role`); the actual IAM role used an underscore (`glue_lakehouse_role`). The error pointed at S3, not the typo.
- **Fix:** Verified the full ARN character by character in both the Glue console and IAM.
- **Prevention:** Copy-paste role ARNs from IAM — never retype them.

**Athena query syntax errors and engine mismatch**
- **Cause:** The default Athena workgroup uses the Spark engine, which rejects standard SQL syntax.
- **Fix:** Created a new workgroup explicitly configured to the SQL engine.
- **Secondary issue:** Athena SQL engine uses backtick quoting in some contexts where double quotes would work in standard SQL — switched to backticks.

**Glue ETL job failing with "Database not found"**
- **Cause:** Database created as `lakehouse-db` (hyphen); ETL script referenced `lakehouse_db` (underscore). Glue treats these as distinct — no normalization.
- **Fix:** Renamed the database to `lakehouse_db` and updated the crawler config and ETL script to match.
- **Prevention:** Use underscores in all Glue database names. Never use hyphens.

---

## Cost Model

| Service | Billing model | Key control |
|---|---|---|
| Amazon Athena | $5 per TB scanned | Always filter on partition columns (`year`, `month`) in every query |
| Redshift Serverless | Per RPU-second while active | Configure auto-pause (5 min for dev, longer for prod) |
| AWS Glue ETL | Per DPU-hour | Use job bookmarks in prod; sample data during dev |
| AWS Lambda | Free up to 1M invocations/month | Pipeline triggers stay well within free tier |
| Amazon S3 | Per GB stored + request pricing | Keep all services in the same region to eliminate transfer fees |
| Amazon DataZone | Per asset published + per subscription | Low relative to compute; check AWS console before publishing a large catalog |

**Development-specific notes:**
- Glue ETL adds up across test runs. A 2-DPU job for 10 minutes costs ~$0.044. Run against small data samples during development.
- Athena dev queries against small Parquet files cost fractions of a cent — but production queries without partition filters against large datasets scale that $5/TB figure fast.
- Redshift Serverless cold start after auto-pause is a few seconds. Set the auto-pause window based on your tolerance for that latency on first query.

---

## What's Next

The current pipeline is a linear flow from raw CSV to curated Parquet to warehouse with governance on top. These extensions move it toward production-hardened:

**Orchestration with Step Functions or MWAA**
When the pipeline has multi-step dependencies — validate quality before promoting to curated, load dimensions before the fact table, notify on failure — a single Lambda function isn't enough. AWS Step Functions handles branching, retries with backoff, parallel steps, and error routing declaratively. MWAA is the better fit when the team already has Airflow DAGs or Airflow expertise.

**Incremental processing with Glue job bookmarks**
The current ETL rewrites the entire curated zone on every run. Job bookmarks track which S3 objects have already been processed so subsequent runs handle only new files — dramatically faster and cheaper once the initial historical load is done.

**Data quality enforcement with Glue Data Quality**
Before data is promoted from raw to curated, automated checks should verify:
- Row counts are within expected ranges
- Key columns have no nulls
- Foreign key values in the fact table exist in dimension tables
- `total_amount` matches `quantity × unit_price`

Glue Data Quality defines these rules declaratively and fails the ETL job automatically if any rule is violated — bad data never reaches the warehouse silently.

**Column-level security in DataZone**
The current setup controls access at the table level. `dim_customer` contains PII — customer names and cities — that should only be visible to specific teams. DataZone with Lake Formation supports column-level masking and restriction policies that hide or hash sensitive fields for unauthorized users without duplicating the table.

**Apache Iceberg for mutable data**
Parquet with Hive-style partitioning is append-only — it doesn't support efficient row-level updates or deletes. If the use case requires correcting historical records (e.g., order cancellations updating past rows), Iceberg on S3 provides ACID transactions, row-level deletes, schema evolution, and time-travel queries without rewriting entire partitions. Both Athena and Redshift Spectrum support Iceberg natively.
