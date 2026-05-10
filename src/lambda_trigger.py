import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')

# Name of the Glue ETL job to trigger.
# Must match the job name in the Glue console exactly.
GLUE_JOB_NAME = 'lakehouse-etl-job'

# Only files under this prefix trigger the pipeline.
# Prevents ETL output writes to curated/ from causing infinite loops.
TARGET_PREFIX = 'raw/orders/'


def lambda_handler(event, context):
    """
    Triggered by S3 PutObject events when a new file lands in the raw zone.
    Extracts file details from the event payload and starts the Glue ETL job.
    """
    logger.info(f"Received event: {json.dumps(event)}")

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = record['s3']['object']['key']
        logger.info(f"New file detected: s3://{bucket}/{key}")

        # Guard: only process files in the expected raw orders path
        if not key.startswith(TARGET_PREFIX):
            logger.info(f"Key {key!r} does not match target prefix {TARGET_PREFIX!r}. Skipping.")
            continue

        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--source_bucket': bucket,
                    '--source_key':    key,
                }
            )
            job_run_id = response['JobRunId']
            logger.info(f"Started Glue job '{GLUE_JOB_NAME}' — run ID: {job_run_id}")

        except Exception as e:
            logger.error(f"Failed to start Glue job: {e}")
            raise

    return {
        'statusCode': 200,
        'body': json.dumps('Pipeline trigger complete.')
    }
