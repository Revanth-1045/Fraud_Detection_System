import boto3
import time
import sys

AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"
S3_ENDPOINT_URL = "http://localhost:9000"
BUCKET = "models"
KEY = "model.json"

def check_model():
    print(f"Polling s3://{BUCKET}/{KEY} every 10s...")
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    timeout = 300 # 5 minutes
    start = time.time()
    
    while time.time() - start < timeout:
        try:
            s3.head_object(Bucket=BUCKET, Key=KEY)
            print("✅ Model found in S3!")
            return True
        except:
            pass # Not found
        
        print(".", end="", flush=True)
        time.sleep(10)
        
    print("\n❌ Timeout waiting for model.")
    return False

if __name__ == "__main__":
    if not check_model():
        sys.exit(1)
