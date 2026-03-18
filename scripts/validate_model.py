import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, confusion_matrix
import sys
import os

def validate(model_path, test_data_path):
    if not os.path.exists(model_path):
        print(f"Error: Model not found at {model_path}")
        return
        
    if not os.path.exists(test_data_path):
        print(f"Error: Test data not found at {test_data_path}")
        return

    # Load test data
    df = pd.read_csv(test_data_path)

    # Load model
    model = xgb.XGBClassifier()
    model.load_model(model_path)

    # Prepare features
    # Ensure these match config.yaml
    features = ["tx_count_1h", "avg_diff_ratio", "amount", "is_high_risk_hr"]
    
    # Check if features exist
    if not all(col in df.columns for col in features):
        print(f"Error: Missing columns in test data. Required: {features}")
        return

    X = df[features]
    y_true = df["target_fraud"]

    # Predict
    y_pred = model.predict(X)

    # Report
    print("=== Model Performance ===")
    print(classification_report(y_true, y_pred))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_true, y_pred))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python validate_model.py <model_path> <test_data_path>")
        sys.exit(1)
        
    validate(sys.argv[1], sys.argv[2])
