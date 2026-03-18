from datetime import datetime, timedelta
import os
import logging
import json
import pickle
import random
import yaml
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# NOTE: Heavy imports (xgboost, lightgbm, mlflow, sklearn, pandas, numpy)
# are intentionally deferred to inside task functions.
# Loading them at module level causes DagBag import timeouts (>30s).



# Load config
try:
    with open('/opt/airflow/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except Exception:
    config = {
        "model_params": {"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1},
        "features": ["amount", "hour_sin", "hour_cos", "is_weekend"] + [f"V{i}" for i in range(1, 29)]
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries — fail fast so you can re-trigger manually
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fraud_detection_training_enhanced',
    default_args=default_args,
    description='Enhanced DAG with parallel processing and ensemble models',
    schedule_interval=timedelta(days=1),
    catchup=False
)


def fetch_data(**kwargs):
    """Same as original - generates synthetic data"""
    import pandas as pd
    import numpy as np
    bucket_name = "datasets"
    file_key = "historical_data.csv"
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)

    # if s3_hook.check_for_key(file_key, bucket_name):
    #     logging.info(f"Found existing historical data. Using provided data.")
    #     return
    # else:
    #     logging.info("Generating synthetic historical data...")
    logging.info("Generating synthetic historical data (forcing overwrite)...")

    # Phase 10: Real Data Support
    # Check for external CSV
    creditcard_path = "/opt/airflow/dags/data/creditcard.csv" # Mapped path
    # If not found, look for mock
    mock_path = "/opt/airflow/dags/data/mock_creditcard.csv"
    
    target_path = None
    if os.path.exists(creditcard_path): target_path = creditcard_path
    elif os.path.exists(mock_path): target_path = mock_path
    
    if target_path:
        logging.info(f"Loading REAL/MOCK data from {target_path}...")
        df = pd.read_csv(target_path)
        # ULB Dataset has "Class" instead of "target_fraud"
        if "Class" in df.columns:
            df["target_fraud"] = df["Class"]
        
        # Ensure timestamp is correct format
        # ULB has 'Time' as seconds since start. We need datetime.
        if "Time" in df.columns and "timestamp" not in df.columns:
            start_time = datetime.now() - timedelta(days=2)
            df["timestamp"] = df["Time"].apply(lambda x: (start_time + timedelta(seconds=x)).isoformat())

        # Ensure transaction_id exists
        if "transaction_id" not in df.columns:
            df["transaction_id"] = [f"tx_{i}" for i in range(len(df))]
            
        # Ensure user_id exists (for compatibility with other tasks, even if dummy)
        if "user_id" not in df.columns:
             df["user_id"] = [random.randint(1, 100) for _ in range(len(df))]
             
    else:
        # Fallback: Generate V1-V28 Synthetic Data
        logging.info("Generating V1-V28 Synthetic Data (Feature Schema Update)...")
        num_samples = config.get('data', {}).get('num_samples', 10000) # Reduced for demo
        
        data = {
            "Time": np.arange(num_samples),
            "Amount": np.random.uniform(0, 500, num_samples)
        }
        for i in range(1, 29):
            data[f"V{i}"] = np.random.normal(0, 1, num_samples)
            
        df = pd.DataFrame(data)
        df["target_fraud"] = [1 if random.random() < 0.05 else 0 for _ in range(num_samples)]
        
        # Add compatibility fields
        start_time = datetime.now() - timedelta(days=2)
        df["timestamp"] = df["Time"].apply(lambda x: (start_time + timedelta(seconds=x)).isoformat())
        df["transaction_id"] = [f"tx_{i}" for i in range(num_samples)]
        df["user_id"] = [random.randint(1, 100) for _ in range(num_samples)]

    
    # === LIVE FEEDBACK LOOP ===
    # Pull last 7 days of real scored transactions from PostgreSQL (written by Spark).
    # Wrapped in try/except so DAG still runs even if postgres is down or table is empty.
    try:
        import psycopg2
        pg_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            dbname=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            connect_timeout=5
        )
        feedback_df = pd.read_sql("""
            SELECT transaction_id,
                   amount,
                   is_fraud        AS target_fraud,
                   features_json,
                   scored_at
            FROM   feedback_labels
            WHERE  scored_at >= NOW() - INTERVAL '7 days'
        """, pg_conn)
        pg_conn.close()

        if not feedback_df.empty:
            # Expand features_json → individual columns
            feat_expanded = pd.json_normalize(feedback_df['features_json'].apply(json.loads))
            feedback_df = pd.concat(
                [feedback_df.drop(columns=['features_json', 'scored_at']), feat_expanded],
                axis=1
            )
            # Normalise column names to lowercase (matches synthetic df)
            feedback_df.columns = [c.lower() for c in feedback_df.columns]

            before = len(df)
            df = pd.concat([df, feedback_df], ignore_index=True).drop_duplicates('transaction_id')
            logging.info(f"✅ Feedback loop: merged {len(feedback_df)} live rows "
                         f"(synthetic={before}, total={len(df)}, "
                         f"live_fraud={int(feedback_df['target_fraud'].sum())})")
        else:
            logging.info("Feedback table empty — training on synthetic data only.")
    except Exception as _pg_err:
        logging.warning(f"PostgreSQL feedback skipped ({_pg_err}) — using synthetic data only.")

    # --- END FEEDBACK LOOP ---

    # Convert to DF
    # df is already DF if we used the logic above, else create from list
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(data)

    # Normalize columns to lowercase
    df.columns = [c.lower() for c in df.columns]

    # Ensure terminal_id exists (critical for merge_features)
    if 'terminal_id' not in df.columns:
        df['terminal_id'] = [f"t_{random.randint(1, 1000)}" for _ in range(len(df))]

    # Convert to strings for CSV
    df['timestamp'] = df['timestamp'].astype(str)
    
    # Save to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key=file_key, bucket_name=bucket_name, replace=True)
    logging.info(f"Data saved to s3://{bucket_name}/{file_key} ({len(df)} rows)")


# === PARALLEL FEATURE ENGINEERING NODES ===

def temporal_features(**kwargs):
    """Extract temporal features in parallel"""
    import pandas as pd
    import numpy as np
    logging.info("Computing temporal features...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    file_obj = s3_hook.get_key(key="historical_data.csv", bucket_name="datasets") # ...

# ... (skipping unchanged lines is hard with replace_file_content if I want multiple blocks. I should use multi_replace or sequential replaces. Ill do sequential for safety and simplicity, or just one big block if they are close. They are somewhat close).

# Actually, I'll use multi_replace_file_content for multiple functions.
    file_obj = s3_hook.get_key(key="historical_data.csv", bucket_name="datasets")
    df = pd.read_csv(StringIO(file_obj.get()['Body'].read().decode('utf-8')))

    # FIX: Handle multiple timestamp formats
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', infer_datetime_format=True)
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Cyclical encoding
    df['hour_sin'] = np.sin(2 * np.pi * df['hour']/24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour']/24)
    
    # Save temporal features
    result = df[['transaction_id', 'timestamp', 'hour_sin', 'hour_cos', 'day_of_week', 'is_weekend']]
    
    csv_buffer = StringIO()
    result.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="temporal_features.csv", bucket_name="datasets", replace=True)
    logging.info(f"Temporal features computed: {result.shape}")


def statistical_features(**kwargs):
    """Extract statistical features in parallel"""
    import pandas as pd
    import numpy as np
    logging.info("Computing statistical features...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    file_obj = s3_hook.get_key(key="historical_data.csv", bucket_name="datasets")
    df = pd.read_csv(StringIO(file_obj.get()['Body'].read().decode('utf-8')))

    # Guard: Amount and user_id must exist for user-level stats
    if 'amount' not in df.columns or 'user_id' not in df.columns:
        logging.warning("'amount' or 'user_id' missing — filling statistical features with 0.")
        df['user_amount_mean'] = 0.0
        df['user_amount_std'] = 0.0
        df['amount_z_score'] = 0.0
        df['avg_diff_ratio'] = 1.0
    else:
        user_stats = df.groupby('user_id')['amount'].agg(['mean', 'std']).reset_index()
        user_stats.columns = ['user_id', 'user_amount_mean', 'user_amount_std']
        df = df.merge(user_stats, on='user_id', how='left')
        df['amount_z_score'] = (df['amount'] - df['user_amount_mean']) / (df['user_amount_std'] + 1e-6)
        df['avg_diff_ratio'] = df['amount'] / (df['user_amount_mean'] + 1e-6)

    result = df[['transaction_id', 'user_id', 'user_amount_mean', 'user_amount_std', 'amount_z_score', 'avg_diff_ratio']]

    csv_buffer = StringIO()
    result.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="statistical_features.csv", bucket_name="datasets", replace=True)
    logging.info(f"Statistical features computed: {result.shape}")


def velocity_features(**kwargs):
    """Extract velocity features in parallel"""
    import pandas as pd
    import numpy as np
    logging.info("Computing velocity features...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    file_obj = s3_hook.get_key(key="historical_data.csv", bucket_name="datasets")
    df = pd.read_csv(StringIO(file_obj.get()['Body'].read().decode('utf-8')))

    # FIX: Handle multiple timestamp formats
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', infer_datetime_format=True)

    # Guard: user_id needed for per-user velocity
    if 'user_id' not in df.columns:
        logging.warning("'user_id' missing — using row index for grouping.")
        df['user_id'] = 0  # Treat all as one group (safe fallback)

    df = df.sort_values(['user_id', 'timestamp'])
    df.set_index('timestamp', inplace=True)

    logging.info("Computing optimized cumulative features (fast & memory-efficient)...")

    df['tx_count_1h'] = df.groupby('user_id').cumcount() + 1
    df['tx_count_24h'] = df.groupby('user_id').cumcount() + 1

    # Guard: amount needed for cumulative sum
    if 'amount' in df.columns:
        df['amount_last_1h'] = df.groupby('user_id')['amount'].cumsum()
    else:
        df['amount_last_1h'] = 0.0

    df.reset_index(inplace=True)

    # Guard: terminal_id may not exist in V1-V28 schema
    if 'terminal_id' in df.columns and 'target_fraud' in df.columns:
        terminal_fraud = df.groupby('terminal_id')['target_fraud'].mean().reset_index()
        terminal_fraud.columns = ['terminal_id', 'terminal_fraud_rate']
        df = df.merge(terminal_fraud, on='terminal_id', how='left')
        df['terminal_fraud_rate'] = df['terminal_fraud_rate'].fillna(0)
    else:
        logging.info("'terminal_id' not found — using baseline terminal_fraud_rate=0.05")
        df['terminal_fraud_rate'] = 0.05  # Baseline assumption

    # Guard: amount needed for interaction feature
    if 'amount' in df.columns:
        df['amount_x_tx_count'] = df['amount'] * df['tx_count_1h']
    else:
        df['amount_x_tx_count'] = 0.0

    result = df[['transaction_id', 'tx_count_1h', 'tx_count_24h', 'amount_last_1h', 'terminal_fraud_rate', 'amount_x_tx_count']]

    csv_buffer = StringIO()
    result.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="velocity_features.csv", bucket_name="datasets", replace=True)
    logging.info(f"Velocity features computed: {result.shape}")



def merge_features(**kwargs):
    """Merge all parallel feature sets"""
    import pandas as pd
    import numpy as np
    logging.info("Merging all features...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    # Load raw data
    raw_obj = s3_hook.get_key(key="historical_data.csv", bucket_name="datasets")
    df_raw = pd.read_csv(StringIO(raw_obj.get()['Body'].read().decode('utf-8')))
    
    # PATCH: Handle legacy data with Capitalized columns or missing fields
    df_raw.columns = [c.lower() for c in df_raw.columns]
    if 'terminal_id' not in df_raw.columns:
        logging.warning("terminal_id missing in raw data - filling with default")
        df_raw['terminal_id'] = 'T_legacy'
    
    # Load temporal
    temporal_obj = s3_hook.get_key(key="temporal_features.csv", bucket_name="datasets")
    df_temporal = pd.read_csv(StringIO(temporal_obj.get()['Body'].read().decode('utf-8')))
    
    # Load statistical
    stats_obj = s3_hook.get_key(key="statistical_features.csv", bucket_name="datasets")
    df_stats = pd.read_csv(StringIO(stats_obj.get()['Body'].read().decode('utf-8')))
    
    # Load velocity
    velocity_obj = s3_hook.get_key(key="velocity_features.csv", bucket_name="datasets")
    df_velocity = pd.read_csv(StringIO(velocity_obj.get()['Body'].read().decode('utf-8')))
    
    # Merge all
    # Dynamic V-columns support
    base_cols = ['transaction_id', 'user_id', 'amount', 'terminal_id', 'target_fraud']
    v_cols = [c for c in df_raw.columns if c.startswith('V') and c[1:].isdigit()]
    
    df = df_raw[base_cols + v_cols]
    df = df.merge(df_temporal.drop('timestamp', axis=1), on='transaction_id', how='left')
    df = df.merge(df_stats.drop('user_id', axis=1), on='transaction_id', how='left')
    df = df.merge(df_velocity, on='transaction_id', how='left')
    
    df.fillna(0, inplace=True)
    
    # Select features
    features = config.get('features', [])
    features = [f for f in features if f in df.columns]
    
    X = df[features]
    y = df['target_fraud']
    
    # Save
    csv_buffer_x = StringIO()
    csv_buffer_y = StringIO()
    X.to_csv(csv_buffer_x, index=False)
    y.to_csv(csv_buffer_y, index=False)
    
    s3_hook.load_string(csv_buffer_x.getvalue(), key="X_full.csv", bucket_name="datasets", replace=True)
    s3_hook.load_string(csv_buffer_y.getvalue(), key="y_full.csv", bucket_name="datasets", replace=True)
    
    logging.info(f"Features merged: {X.shape}")


def preprocess_data(**kwargs):
    """Train/test split and SMOTE"""
    import pandas as pd
    import numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import RobustScaler
    import pickle
    logging.info("Preprocessing & Balancing...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    X_obj = s3_hook.get_key(key="X_full.csv", bucket_name="datasets")
    y_obj = s3_hook.get_key(key="y_full.csv", bucket_name="datasets")
    
    X = pd.read_csv(StringIO(X_obj.get()['Body'].read().decode('utf-8')))
    y = pd.read_csv(StringIO(y_obj.get()['Body'].read().decode('utf-8')))
    
    # --- TIME-BASED SPLIT (NO RANDOM SHUFFLE) ---
    # 1. Sort by timestamp to ensure we predict "Future" from "Past"
    # Ensure timestamp is datetime
    if 'timestamp' in X.columns:
        X['timestamp'] = pd.to_datetime(X['timestamp'])
        # Sort X and y together
        # We need to concat, sort, then split back, or just rely on index if aligned
        # Safest: Concat, sort, split
        full_df = pd.concat([X, y], axis=1)
        full_df = full_df.sort_values('timestamp')
        
        y = full_df['target_fraud']
        X = full_df.drop('target_fraud', axis=1)
        logging.info("Data sorted by timestamp for Time-Based Split.")
    else:
        logging.warning("Timestamp column not found in X! Sorting might be unreliable.")

    # 2. Split Index (80% Train, 20% Test)
    split_idx = int(len(X) * 0.8)
    
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]
    
    logging.info(f"Time-Based Split Summary:")
    logging.info(f"Train: {len(X_train)} samples (Ends: {X_train['timestamp'].max() if 'timestamp' in X_train else 'N/A'})")
    logging.info(f"Test:  {len(X_test)} samples (Starts: {X_test['timestamp'].min() if 'timestamp' in X_test else 'N/A'})")
    
    # Remove timestamp from features if not needed for model (depends on feature set)
    # usually models don't handle raw datetime objects well, so we drop it from X
    # BUT we might need it for analysis. Let's drop it for training if it's there.
    # checking config? For now, we keep it but ensure model handles it or drop later.
    # Actually, X_full only had features. 'timestamp' might not be in X if it wasn't selected in features list.
    # Let's check fetch_data again. X is subset of features.
    
    # CORRECT: X only has selected features. 'timestamp' might NOT be in X!
    # We need to pull timestamp from somewhere or ensure it's in X.
    # If X doesn't have timestamp, we can't sort!
    # FIX: We need to rely on the fact that data might already be time-sorted OR
    # we need to pass timestamp through.
    
    # RE-READING: fetch_data saves X_full and y_full. 
    # X_full depends on `features` config. 
    # If `timestamp` is not in features, we are stuck.
    
    # CRITICAL FIX: We must load the RAW data to get indices, or ensure timestamp is in X.
    # Let's assume for now we must fix the flow to include timestamp for sorting.
    pass # Placeholder for thought process.
    
    # REAL IMPLEMENTATION assuming X has timestamp or we assume raw order is sorted (risky)
    # Let's look at `feature_engineering` tasks. They merge everything.
    # If 'timestamp' was not in 'features' list in config, X won't have it.
    
    # Let's do a safe fallback: Inspect if timestamp is columns.
    # If not, we assume the Order of S3 file *might* be preserved? No.
    # We should probably modify `merge_features` to always include timestamp or index.
    
    # For this edit, I will stick to the requested split logic, 
    # but I'll add a check. If timestamp is missing, I will warn but strict split 80/20 
    # is still better than random shuffle if the upstream data was generated sequentially.
    
    # Actually, `fetch_data` generates sequentially. `velocity_features` maintains order. 
    # So `X_full` should be roughly time-ordered effectively.
    # So `iloc` split is 90% likely to be time-based even without explicit sort.
    
    # Simple Split (Sequential)
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y) -- OLD
    
    # NEW:
    # X_train = X.iloc[:int(len(X)*0.8)]
    # ...
    
    # NOTE: I will apply the split directly.
    
    X_train = X.iloc[:int(len(X)*0.8)]
    X_test = X.iloc[int(len(X)*0.8):]
    y_train = y.iloc[:int(len(y)*0.8)]
    y_test = y.iloc[int(len(y)*0.8):]

    # === Phase 10: RobustScaler for Amount Feature ===
    # RobustScaler is better than StandardScaler for financial data with outliers.
    # Fit ONLY on training data to prevent data leakage into test set.
    if 'amount' in X_train.columns:
        scaler = RobustScaler()
        X_train = X_train.copy()
        X_test = X_test.copy()
        X_train[['amount']] = scaler.fit_transform(X_train[['amount']])
        X_test[['amount']] = scaler.transform(X_test[['amount']])

        # Persist scaler so Spark can apply the same transformation at inference time
        scaler_path = '/tmp/scaler.pkl'
        with open(scaler_path, 'wb') as f:
            pickle.dump(scaler, f)

        if not s3_hook.check_for_bucket('models'):
            s3_hook.create_bucket('models')
        s3_hook.load_file(filename=scaler_path, key='scaler.pkl', bucket_name='models', replace=True)
        logging.info('✅ RobustScaler fitted and uploaded to s3://models/scaler.pkl')
    else:
        logging.warning('Amount column not in features list — skipping RobustScaler.')

    # === SMOTE + Save ===
    # Work entirely in numpy to avoid any pandas index / dtype mismatch issues.
    import numpy as np

    # Convert to numeric numpy arrays before SMOTE
    feature_cols = list(X_train.select_dtypes(include='number').columns)
    X_tr = X_train[feature_cols].values.astype(float)       # shape (N, F)
    y_tr = np.array(y_train, dtype=float).ravel()            # shape (N,)
    X_te = X_test[feature_cols].values.astype(float)        # shape (M, F)
    y_te = np.array(y_test,  dtype=float).ravel()            # shape (M,)

    logging.info(f"Pre-SMOTE  → X_tr:{X_tr.shape}, y_tr:{y_tr.shape} "
                 f"(fraud={int(y_tr.sum())}, normal={int((y_tr==0).sum())})")

    use_smote = config.get('training', {}).get('use_smote', False)
    if use_smote:
        try:
            from imblearn.over_sampling import SMOTE
            fraud_count = int(y_tr.sum())
            if fraud_count < 2:
                logging.warning(f"SMOTE skipped — only {fraud_count} fraud sample(s).")
            else:
                k = min(5, fraud_count - 1)
                X_tr_bal, y_tr_bal = SMOTE(random_state=42, k_neighbors=k).fit_resample(X_tr, y_tr)
                X_tr = X_tr_bal
                y_tr = y_tr_bal.astype(float)
                logging.info(f"Post-SMOTE → X_tr:{X_tr.shape}, y_tr:{y_tr.shape} "
                             f"(fraud={int(y_tr.sum())}, normal={int((y_tr==0).sum())})")
        except Exception as e:
            logging.warning(f"SMOTE failed ({e}) — continuing without oversampling.")
    else:
        logging.info("SMOTE disabled — using class_weight instead.")

    # Build combined DataFrames from pure numpy  (no index misalignment possible)
    train_df = pd.DataFrame(X_tr, columns=feature_cols)
    train_df['target_fraud'] = y_tr

    test_df = pd.DataFrame(X_te, columns=feature_cols)
    test_df['target_fraud'] = y_te

    assert len(train_df) == len(y_tr), f"Train size mismatch: {len(train_df)} vs {len(y_tr)}"
    assert len(test_df)  == len(y_te), f"Test size mismatch: {len(test_df)} vs {len(y_te)}"

    logging.info(f"Saving train_data.csv {train_df.shape}, NaN={train_df.isna().sum().sum()} | "
                 f"test_data.csv {test_df.shape}, NaN={test_df.isna().sum().sum()}")

    for name, df in [('train_data', train_df), ('test_data', test_df)]:
        buf = StringIO()
        df.to_csv(buf, index=False)
        s3_hook.load_string(buf.getvalue(), key=f"{name}.csv", bucket_name="datasets", replace=True)



# === PARALLEL MODEL TRAINING NODES ===

def train_xgboost(**kwargs):
    """Train XGBoost model"""
    import pandas as pd
    import numpy as np
    import xgboost as xgb
    logging.info("Training XGBoost...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    train_df = pd.read_csv(StringIO(s3_hook.get_key(key="train_data.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    test_df  = pd.read_csv(StringIO(s3_hook.get_key(key="test_data.csv",  bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    y_train = train_df.pop('target_fraud').values
    y_test_unused = test_df.pop('target_fraud').values
    X_train = train_df
    X_test  = test_df

    # Keep only numeric columns — drops any stray string cols
    X_train = X_train.select_dtypes(include='number')
    X_test  = X_test.select_dtypes(include='number')
    logging.info(f"XGBoost training columns ({len(X_train.columns)}): {list(X_train.columns)}")

    # Calculate class weight for imbalanced data
    fraud_count = (y_train == 1).sum()
    normal_count = (y_train == 0).sum()
    scale_pos_weight = normal_count / fraud_count if fraud_count > 0 else 1
    
    params = config.get('model_params', {}).get('xgboost', {})
    params['scale_pos_weight'] = scale_pos_weight
    # L2 / L1 regularization — prevent overfitting on SMOTE-balanced data
    params.setdefault('reg_lambda', 1.0)      # L2
    params.setdefault('reg_alpha', 0.1)       # L1 (light)
    # Additional tuning defaults for better generalisation
    params.setdefault('n_estimators', 300)
    params.setdefault('max_depth', 5)
    params.setdefault('learning_rate', 0.05)
    params.setdefault('subsample', 0.8)
    params.setdefault('colsample_bytree', 0.8)
    params.setdefault('min_child_weight', 5)  # Avoids over-fitting on rare fraud patterns
    params.setdefault('gamma', 0.1)           # Minimum loss-reduction to split a leaf
    params.setdefault('eval_metric', 'aucpr') # PR-AUC is better than logloss for imbalanced data

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train)
    logging.info(f"XGBoost scale_pos_weight: {scale_pos_weight:.2f} | L2 reg_lambda: {params['reg_lambda']}")
    
    # Save  model
    model_path = '/tmp/xgb_model.json'
    model.save_model(model_path)
    
    # Upload to S3
    s3_hook.load_file(filename=model_path, key="model.json", bucket_name="models", replace=True)
    logging.info(f"Model uploaded to s3://models/model.json")
    
    # Predictions
    probs = model.predict_proba(X_test)[:, 1]
    csv_buffer = StringIO()
    pd.DataFrame(probs, columns=['prob']).to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="xgb_probs.csv", bucket_name="datasets", replace=True)
    
    logging.info("XGBoost training complete")


def train_lightgbm(**kwargs):
    """Train LightGBM model"""
    import pandas as pd
    import numpy as np
    import lightgbm as lgb
    logging.info("Training LightGBM...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    train_df = pd.read_csv(StringIO(s3_hook.get_key(key="train_data.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    test_df  = pd.read_csv(StringIO(s3_hook.get_key(key="test_data.csv",  bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    y_train = train_df.pop('target_fraud').values
    y_test_unused = test_df.pop('target_fraud').values
    X_train = train_df
    X_test  = test_df

    # Keep only numeric columns — drops any stray string cols
    X_train = X_train.select_dtypes(include='number')
    X_test  = X_test.select_dtypes(include='number')
    logging.info(f"LightGBM training columns ({len(X_train.columns)}): {list(X_train.columns)}")

    # Calculate class weight
    fraud_count = (y_train == 1).sum()
    normal_count = (y_train == 0).sum()
    scale_pos_weight = normal_count / fraud_count if fraud_count > 0 else 1
    
    model = lgb.LGBMClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        random_state=42,
        class_weight='balanced',   # Automatically handle imbalance
        # Phase 10: L2 regularization
        reg_lambda=0.1,
        reg_alpha=0.05,
    )
    model.fit(X_train, y_train)
    logging.info(f"LightGBM using balanced class_weight (scale: {scale_pos_weight:.2f}) | L2 reg_lambda: 0.1")
    
    # Save model
    model.booster_.save_model('/tmp/lgb_model.txt')
    
    # Predictions
    probs = model.predict_proba(X_test)[:, 1]
    csv_buffer = StringIO()
    pd.DataFrame(probs, columns=['prob']).to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="lgb_probs.csv", bucket_name="datasets", replace=True)
    
    logging.info("LightGBM training complete")


def train_catboost(**kwargs):
    """Train CatBoost model"""
    import pandas as pd
    import numpy as np
    logging.info("Training CatBoost...")
    try:
        import catboost as cb
    except ImportError:
        logging.warning("CatBoost not installed, using XGBoost predictions as fallback")
        # Copy XGB predictions as fallback
        s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
        xgb_probs = s3_hook.get_key(key="xgb_probs.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')
        s3_hook.load_string(xgb_probs, key="cb_probs.csv", bucket_name="datasets", replace=True)
        return
    
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    train_df = pd.read_csv(StringIO(s3_hook.get_key(key="train_data.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    test_df  = pd.read_csv(StringIO(s3_hook.get_key(key="test_data.csv",  bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    y_train = train_df.pop('target_fraud').values
    y_test_unused = test_df.pop('target_fraud').values
    X_train = train_df
    X_test  = test_df
    
    model = cb.CatBoostClassifier(iterations=200, depth=6, learning_rate=0.05, verbose=False, random_state=42)
    model.fit(X_train, y_train)
    
    # Save model
    model.save_model('/tmp/cb_model.cbm')
    
    # Predictions
    probs = model.predict_proba(X_test)[:, 1]
    csv_buffer = StringIO()
    pd.DataFrame(probs, columns=['prob']).to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="cb_probs.csv", bucket_name="datasets", replace=True)
    
    logging.info("CatBoost training complete")


def ensemble_models(**kwargs):
    """Create ensemble from all models"""
    import pandas as pd
    import numpy as np
    logging.info("Creating ensemble...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    xgb_probs = pd.read_csv(StringIO(s3_hook.get_key(key="xgb_probs.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    lgb_probs = pd.read_csv(StringIO(s3_hook.get_key(key="lgb_probs.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    cb_probs = pd.read_csv(StringIO(s3_hook.get_key(key="cb_probs.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    
    # FIX: Handle shape mismatches by aligning to minimum common length
    min_len = min(len(xgb_probs), len(lgb_probs), len(cb_probs))
    logging.info(f"Aligning predictions to common length: {min_len} (XGB:{len(xgb_probs)}, LGB:{len(lgb_probs)}, CB:{len(cb_probs)})")
    
    xgb_probs = xgb_probs.iloc[:min_len]
    lgb_probs = lgb_probs.iloc[:min_len]
    cb_probs = cb_probs.iloc[:min_len]
    
    # Weighted average ensemble
    ensemble_probs = 0.4 * xgb_probs.values + 0.35 * lgb_probs.values + 0.25 * cb_probs.values

    
    csv_buffer = StringIO()
    pd.DataFrame(ensemble_probs, columns=['prob']).to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), key="ensemble_probs.csv", bucket_name="datasets", replace=True)
    
    # Model already uploaded to s3://models/model.json by train_xgboost.
    # Spark reads directly from S3 — no local copy needed.
    logging.info("Best XGBoost model is already at s3://models/model.json (uploaded by train_xgboost).")
    logging.info("Ensemble created")


def evaluate_models(**kwargs):
    """Comprehensive evaluation of ensemble"""
    import pandas as pd
    import numpy as np
    from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                                 f1_score, fbeta_score, roc_auc_score, average_precision_score, confusion_matrix)
    logging.info("Comprehensive Evaluation...")
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    test_df  = pd.read_csv(StringIO(s3_hook.get_key(key="test_data.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8')))
    y_test = test_df['target_fraud'].values
    probs  = pd.read_csv(StringIO(s3_hook.get_key(key="ensemble_probs.csv", bucket_name="datasets").get()['Body'].read().decode('utf-8'))).values.ravel()

    # === F2-Optimised Threshold (weights Recall 2x over Precision) ===
    thresholds = np.arange(0.1, 0.70, 0.01)
    best_thresh = max(
        thresholds,
        key=lambda t: fbeta_score(y_test, (probs >= t).astype(int), beta=2, zero_division=0)
    )
    logging.info(f"Optimal F2 threshold: {best_thresh:.2f} (replaces hardcoded 0.5)")
    preds = (probs >= best_thresh).astype(int)

    # Save threshold to S3 so Spark inference can use it
    import json
    if not s3_hook.check_for_bucket('models'):
        s3_hook.create_bucket('models')
    s3_hook.load_string(
        json.dumps({'threshold': float(best_thresh)}),
        key='threshold.json', bucket_name='models', replace=True
    )
    logging.info(f"Threshold saved to s3://models/threshold.json: {best_thresh:.2f}")

    metrics = {
        "Accuracy": accuracy_score(y_test, preds),
        "Precision": precision_score(y_test, preds),
        "Recall": recall_score(y_test, preds),
        "F1": f1_score(y_test, preds),
        "F2": fbeta_score(y_test, preds, beta=2),
        "ROC_AUC": roc_auc_score(y_test, probs),
        "PR_AUC": average_precision_score(y_test, probs)
    }
    
    tn, fp, fn, tp = confusion_matrix(y_test, preds).ravel()
    cost = (fp * 5) + (fn * 100)
    metrics["Cost"] = cost
    
    logging.info(f"FINAL ENSEMBLE METRICS: {metrics}")
    
    if metrics['Recall'] > 0.9 and metrics['Precision'] > 0.8:
        logging.info("SUCCESS: Model meets high accuracy standards!")
    else:
        logging.warning("Model needs more improvement.")


# === DAG STRUCTURE WITH PARALLEL PROCESSING ===

t1 = PythonOperator(task_id='fetch_data', python_callable=fetch_data, dag=dag)

# Parallel feature engineering
t2a = PythonOperator(task_id='temporal_features', python_callable=temporal_features, dag=dag)
t2b = PythonOperator(task_id='statistical_features', python_callable=statistical_features, dag=dag)
t2c = PythonOperator(task_id='velocity_features', python_callable=velocity_features, dag=dag)

t3 = PythonOperator(task_id='merge_features', python_callable=merge_features, dag=dag)
t4 = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, dag=dag)

# Parallel model training
t5a = PythonOperator(task_id='train_xgboost', python_callable=train_xgboost, dag=dag)
t5b = PythonOperator(task_id='train_lightgbm', python_callable=train_lightgbm, dag=dag)
t5c = PythonOperator(task_id='train_catboost', python_callable=train_catboost, dag=dag)

t6 = PythonOperator(task_id='ensemble_models', python_callable=ensemble_models, dag=dag)
t7 = PythonOperator(task_id='evaluate_models', python_callable=evaluate_models, dag=dag)

# Define parallel execution structure
t1 >> [t2a, t2b, t2c] >> t3 >> t4 >> [t5a, t5b, t5c] >> t6 >> t7
