import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, round as spark_round

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_DATABASE', 'SOURCE_TABLE', 'TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Extract ──────────────────────────────────────────────────────────────────
# Read from Glue Data Catalog — no hardcoded schema in the script.
# If the source schema changes and the crawler is re-run, the job picks
# up the updated definition automatically.
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args['SOURCE_DATABASE'],
    table_name=args['SOURCE_TABLE']
)

df = dynamic_frame.toDF()

# ── Transform ─────────────────────────────────────────────────────────────────
# 1. Cast types — explicit casts make intent clear and behaviour predictable
#    across Spark versions rather than relying on implicit coercion.
df = df.withColumn("order_date", col("order_date").cast("date"))
df = df.withColumn("quantity",   col("quantity").cast("integer"))
df = df.withColumn("unit_price", col("unit_price").cast("double"))

# 2. Derive columns — pre-compute total_amount once so downstream queries
#    don't repeat the multiplication. year/month become partition keys.
df = df.withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))
df = df.withColumn("year",  year(col("order_date")).cast("string"))
df = df.withColumn("month", month(col("order_date")).cast("string"))

# ── Load ──────────────────────────────────────────────────────────────────────
# Write partitioned Parquet to the curated zone.
# Partitioning by year/month means Athena reads only matching folders —
# directly reducing cost ($5/TB scanned) when filters are applied.
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(args['TARGET_PATH'])

job.commit()
