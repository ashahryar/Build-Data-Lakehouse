# Architecture Notes

Design decisions, tradeoffs, and implementation details for the AWS Data Lakehouse.

---

## Three-Zone S3 Layout

| Zone | Path | Purpose |
|---|---|---|
| Raw | `raw/orders/`, `raw/dimensions/` | Immutable landing zone. Never modified after write. |
| Curated | `curated/orders/year=*/month=*/` | Parquet, partitioned. Output of Glue ETL. |
| Consumption | `consumption/` | Pre-aggregated outputs for specific reporting tools. |

The raw zone is immutable by design. If transformation logic changes, the pipeline can be replayed from scratch without losing the original source data.

---

## IAM Role Design

Three dedicated roles, each scoped to minimum required permissions:

| Role | Used by | Key permissions |
|---|---|---|
| `GlueLakehouseRole` | Glue Crawler + ETL | `AWSGlueServiceRole` (managed) + S3 read/write inline |
| `RedshiftS3ReadRole` | Redshift COPY | S3 `GetObject` + `ListBucket` on lakehouse bucket |
| `LambdaGlueTriggerRole` | Lambda | `AWSLambdaBasicExecutionRole` + `glue:StartJobRun` inline |

All inline policies are scoped to the specific bucket or job ARN — not `*`.

---

## ETL Design Decisions

**Why read from the Glue Data Catalog rather than hardcoding the S3 path?**
If the source schema changes and the crawler is re-run, the ETL job picks up the updated definition automatically. Hardcoding paths or schemas into the script creates a maintenance burden and breaks on schema evolution.

**Why cast `year` and `month` as strings?**
Athena partition column values are strings in Hive-style partitioning (`year=2024/month=1/`). Casting to integer and back inside queries adds noise. Keeping them as strings matches the partition key representation directly.

**Why pre-compute `total_amount` in ETL?**
Avoids redundant `quantity * unit_price` multiplication in every downstream query — both in Redshift and Athena. Compute once, read many times.

**Why `overwrite` mode?**
Allows idempotent re-runs. If the ETL job runs twice against the same input (e.g. Lambda fires twice due to S3 retry), the second run replaces the first output rather than appending duplicate records.

---

## Star Schema Design

```
dim_customer ──┐
               ├── fact_orders ── (measures: quantity, unit_price, total_amount)
dim_product  ──┘
```

The fact table stores foreign keys (`customer_id`, `product_id`) rather than descriptive text. This means:
- A product rename updates only `dim_product` — historical fact rows are unchanged.
- Queries join at runtime — no denormalization to maintain.

**Load order matters:** Dimension tables must be populated before the fact table because of FK constraints. `01_create_schema.sql` creates them in order; `02_load_data.sql` loads them in order.

---

## Athena vs Redshift — When to Use Which

| Scenario | Engine |
|---|---|
| Dashboard queries running on a schedule | Redshift — compiled query plans, consistent sub-second latency |
| One-off investigation or ad-hoc exploration | Athena — no cluster needed, billed per scan |
| Data validation after ETL | Athena — fastest feedback loop |
| Complex multi-table joins at production scale | Redshift — optimized for repeated join patterns |

Both engines read the same curated Parquet files — no sync pipeline, no duplicate storage.

---

## Partition Pruning

Athena charges $5/TB scanned. Without partition filters, a query reads the entire dataset.

```sql
-- Reads ALL data
SELECT SUM(total_amount) FROM orders_curated;

-- Reads ONLY year=2024/month=1/ — ~2.8% of 36-month dataset
SELECT SUM(total_amount) FROM orders_curated
WHERE year = '2024' AND month = '1';
```

Partition columns (`year`, `month`) must appear in every `WHERE` clause against partitioned tables. There is no upside to omitting them.

---

## Lambda Trigger — Idempotency

If the same file is uploaded twice (common in production due to upstream retry logic), Lambda fires twice and the Glue ETL runs twice. This is handled by:

- **Overwrite mode** in the ETL job — the second run replaces the first output, no duplicates created.
- **Prefix filter** in the Lambda handler — only `raw/orders/` uploads trigger the pipeline. Writes to `curated/` from the ETL job itself do not cause a loop.

For production, add Glue job bookmarks to skip already-processed S3 objects entirely.

---

## Known Gotchas

| Issue | Root cause | Fix |
|---|---|---|
| COPY loads 0 rows, no error | Trailing space in S3 path (`"curated /"`) | Verify path character by character; always check `COUNT(*)` after COPY |
| Glue job "Database not found" | Hyphen vs underscore in database name | Use underscores everywhere in Glue database names |
| Athena query syntax errors | Default workgroup uses Spark engine | Create a new workgroup set to SQL engine |
| IAM access denied (S3) | Role name mismatch — hyphen vs underscore in ARN | Copy-paste ARNs from IAM, never retype |
| Redshift VPC error | Fewer than 3 subnets across AZs | Create subnets in 3 different AZs before setup |
| Parquet type mismatch | Spark `INT64` vs Redshift `INTEGER` | Use staging table with VARCHAR columns, then insert with explicit CAST |
