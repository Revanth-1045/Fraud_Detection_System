import boto3
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")

def create_buckets():
    try:
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        buckets = ["datasets", "models"]
        for b in buckets:
            try:
                s3.create_bucket(Bucket=b)
                print(f"Created bucket: {b}")
            except s3.exceptions.BucketAlreadyOwnedByYou:
                print(f"Bucket already exists: {b}")
            except Exception as e:
                print(f"Error creating bucket {b}: {e}")
                
    except Exception as e:
        print(f"Failed to connect to S3: {e}")

if __name__ == "__main__":
    create_buckets()
