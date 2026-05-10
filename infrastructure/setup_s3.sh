#!/usr/bin/env bash
# =============================================================================
# setup_s3.sh — Create the lakehouse S3 bucket and zone structure
#
# Usage:
#   chmod +x setup_s3.sh
#   ./setup_s3.sh <bucket-name>
#
# Example:
#   ./setup_s3.sh ahmed-lakehouse-2024
#
# Requirements:
#   - AWS CLI configured (aws configure)
#   - Bucket name: lowercase, hyphens only, globally unique
# =============================================================================

set -euo pipefail

BUCKET_NAME="${1:-}"

if [[ -z "$BUCKET_NAME" ]]; then
  echo "Usage: $0 <bucket-name>"
  exit 1
fi

echo "Creating S3 bucket: $BUCKET_NAME"
aws s3 mb "s3://${BUCKET_NAME}"

echo "Creating zone prefixes..."
aws s3api put-object --bucket "$BUCKET_NAME" --key raw/
aws s3api put-object --bucket "$BUCKET_NAME" --key curated/
aws s3api put-object --bucket "$BUCKET_NAME" --key consumption/

echo "Uploading sample data to raw zone..."
aws s3 cp data/sample/sample_orders.csv "s3://${BUCKET_NAME}/raw/orders/"
aws s3 cp data/sample/customers.csv     "s3://${BUCKET_NAME}/raw/dimensions/"
aws s3 cp data/sample/products.csv      "s3://${BUCKET_NAME}/raw/dimensions/"

echo ""
echo "Verifying uploads..."
aws s3 ls "s3://${BUCKET_NAME}/raw/" --recursive

echo ""
echo "Done. Bucket structure:"
echo "  s3://${BUCKET_NAME}/raw/orders/sample_orders.csv"
echo "  s3://${BUCKET_NAME}/raw/dimensions/customers.csv"
echo "  s3://${BUCKET_NAME}/raw/dimensions/products.csv"
echo "  s3://${BUCKET_NAME}/curated/   (empty — populated by Glue ETL)"
echo "  s3://${BUCKET_NAME}/consumption/ (empty — for downstream exports)"
