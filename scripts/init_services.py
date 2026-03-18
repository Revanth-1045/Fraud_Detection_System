import boto3
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
MINIO_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

def init_minio():
    logger.info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    buckets = ["datasets", "mlflow-artifacts"]
    
    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            logger.info(f"Bucket {bucket} already exists.")
        except Exception as e:
            logger.error(f"Error creating bucket {bucket}: {e}")

    # Upload historical data if missing
    try:
        # Check if exists
        try:
            s3.head_object(Bucket="datasets", Key="historical_data.csv")
            logger.info("historical_data.csv already exists in S3.")
        except:
            logger.info("Uploading default historical_data.csv...")
            # Create a dummy or real csv
            with open("/tmp/historical_data.csv", "w") as f:
                f.write("transaction_id,user_id,amount,timestamp,target_fraud\n")
                # Just header for now to satisfy check, or we let Airflow generate it.
                # Actually Airflow DAG 'fetch_data' generates it if missing.
                # So we just need the bucket.
                pass
    except Exception as e:
        logger.error(f"Error checking/uploading data: {e}")

def force_start_producer():
    status_file = "/app/data/producer_status"
    try:
        with open(status_file, "w") as f:
            f.write("RUNNING")
        logger.info(f"Forced producer status to RUNNING in {status_file}")
    except Exception as e:
        logger.error(f"Could not write status file: {e}")

if __name__ == "__main__":
    # Wait for services
    time.sleep(2)
    init_minio()
    force_start_producer()
