import boto3
import os

# Configuration
BUCKET_NAME = "datasets"
# Correct path based on docker-compose volume mapping (./src/producer/data:/app/data)
LOCAL_FILE = "src/producer/data/detected_frauds.csv"
S3_KEY = "detected_frauds.csv"
ENDPOINT_URL = "http://localhost:9000"
AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"

def sync_feedback():
    if not os.path.exists(LOCAL_FILE):
        print(f"No feedback file found at {LOCAL_FILE}")
        return

    print(f"Syncing {LOCAL_FILE} to S3://{BUCKET_NAME}/{S3_KEY}...")
    
    s3 = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)
    
    try:
        s3.upload_file(LOCAL_FILE, BUCKET_NAME, S3_KEY)
        print("Sync Complete! Airflow will now see the new frauds.")
    except Exception as e:
        print(f"Error syncing: {e}")

if __name__ == "__main__":
    sync_feedback()
