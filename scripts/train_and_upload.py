import xgboost as xgb
import boto3
import os
import json
import numpy as np
import pickle
from sklearn.preprocessing import RobustScaler

# Config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")

def train_and_upload():
    print("Training dummy model...")
    # Create dummy data matching inference schema:
    # Features: [amount, hour_sin, hour_cos, is_weekend] + V1-V28 (Total 4 + 28 = 32)
    # Amount is at index 0
    X = np.random.rand(100, 32)
    y = np.random.randint(0, 2, 100)
    
    # Phase 10: Apply RobustScaler to 'amount' (column 0)
    print("Fitting RobustScaler on 'amount' feature...")
    scaler = RobustScaler()
    # Fit on the first column (amount)
    # Reshape is needed for a single feature
    X[:, 0] = scaler.fit_transform(X[:, 0].reshape(-1, 1)).flatten()
    
    # Save scaler locally
    with open("scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)
    print("Scaler saved locally as scaler.pkl")

    # Train with L2 Regularization (reg_lambda)
    # reg_lambda=1.0 is default, but making it explicit for Phase 10 req
    print("Training XGBoost with L2 regularization...")
    model = xgb.XGBClassifier(n_estimators=10, reg_lambda=1.0)
    model.fit(X, y)
    
    # Save locally
    model.save_model("model.json")
    print("Model saved locally.")
    
    # Upload to S3
    print(f"Uploading artifacts to {S3_ENDPOINT_URL}...")
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    try:
        # Upload model
        s3.upload_file("model.json", "models", "model.json")
        print("✅ Uploaded model.json")
        
        # Upload scaler
        s3.upload_file("scaler.pkl", "models", "scaler.pkl")
        print("✅ Uploaded scaler.pkl")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    train_and_upload()
