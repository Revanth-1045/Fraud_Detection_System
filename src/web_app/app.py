from flask import Flask, render_template, jsonify
import pandas as pd
import os
import time
try:
    import redis_cache
except ImportError:
    pass # Handle case where running locally without redis installed, though Docker has it

app = Flask(__name__)

app = Flask(__name__)

# Check multiple paths for the data file (Docker vs Local)
POSSIBLE_PATHS = [
    '/app/data/transactions_log.xlsx',  # Docker
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'producer', 'data', 'transactions_log.xlsx'), # Local relative
    '../../producer/data/transactions_log.xlsx' # Local relative fallback
]

DATA_FILE = None
for path in POSSIBLE_PATHS:
    if os.path.exists(os.path.dirname(path)): # Check if directory exists at least
        DATA_FILE = path
        break
if not DATA_FILE:
    DATA_FILE = POSSIBLE_PATHS[0] # Default to Docker path

print(f"Using data file path: {DATA_FILE}")

# Determine Status File Path
# Determine Status File Path
DATA_DIR = os.path.dirname(DATA_FILE) if DATA_FILE else '/app/data'
STATUS_FILE = os.path.join(DATA_DIR, 'producer_status')
# Check shared volume first (Docker), fallback to local data dir
RECENT_FILE = '/shared/dashboard/recent.json' if os.path.exists('/shared/dashboard') else os.path.join(DATA_DIR, 'recent.json')

@app.route('/api/producer/start', methods=['POST'])
def start_producer():
    try:
        with open(STATUS_FILE, 'w') as f:
            f.write("RUNNING")
        return jsonify({"status": "RUNNING", "message": "Producer started."})
    except Exception as e:
        return jsonify({"status": "ERROR", "message": str(e)}), 500

@app.route('/api/producer/stop', methods=['POST'])
def stop_producer():
    try:
        # Ensure directory exists
        if not os.path.exists(os.path.dirname(STATUS_FILE)):
            os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
            
        with open(STATUS_FILE, 'w') as f:
            f.write("STOPPED")
        return jsonify({"status": "STOPPED", "message": "Producer stopped."})
    except Exception as e:
        print(f"Error stopping producer: {e}")
        return jsonify({"status": "ERROR", "message": str(e)}), 500

@app.route('/api/upload_check', methods=['POST'])
def upload_check():
    from flask import request
    
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    if file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls') or file.filename.endswith('.csv')):
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file, engine='openpyxl')
            
            # Basic Validation
            required_cols = ['amount', 'user_id', 'timestamp'] # Minimal requirements
            if not all(col in df.columns for col in required_cols):
                 return jsonify({"error": f"Missing columns. Required: {required_cols}"}), 400

            # Fraud Logic Application
            # 1. Feature Engineering (Simplified for static file)
            # COERCE errors to NaT to handle bad formats, then drop rows with NaT
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])
            
            # Ensure user_id is string for consistent comparison
            df['user_id'] = df['user_id'].astype(str)
            
            # Sort by timestamp to ensure correct history lookup
            df = df.sort_values('timestamp')
            
            df['hour'] = df['timestamp'].dt.hour
            df['is_high_risk_hr'] = df['hour'].apply(lambda x: 1 if 1 <= x <= 5 else 0)
            
            # Simple aggregations for demo (Calculated over the whole file)
            # In real stream this is windowed. Here we take the file as the window or context.
            user_means = df.groupby('user_id')['amount'].mean().to_dict()
            df['avg_amount'] = df['user_id'].map(user_means)
            df['avg_diff_ratio'] = df['amount'] / df['avg_amount']
            
            # Logic: 
            # - Amount > 10000
            # - High Risk Hour AND Amount > 5000
            # - Avg Diff Ratio > 5 (Unusual spike)
            
            # --- BATCH ANALYSIS (Vectorized) ---
            # Calculate metrics for the entire file at once to catch the "whole" scheme
            # (e.g., catch the FIRST transaction of a structuring sequence, not just the last)
            
            # 1. Grouper by User + 1 Hour Window
            # Use set_index for robust time-based rolling
            df_indexed = df.set_index('timestamp').sort_index()
            
            # Calculate rolling sum per user
            hourly_sums = df_indexed.groupby('user_id')['amount'].rolling('1H').sum().reset_index()
            
            # Merge back to original df (careful with duplicates/timestamps)
            # Since we have transaction_id, let's try a different approach to ensure alignment
            # or just map it back if unique.
            
            # Simpler approach:
            # Iterate unique users and calculate, then concat? 
            # Or just use the 'on' parameter correctly: 
            # df.groupby('user_id').rolling('1H', on='timestamp')... requires monotonic index usually
            
            # Let's stick to the set_index method but ensure we can map it back
            # We can use the index to align if we didn't drop rows.
            
            # Actually, standard approach:
            df['hourly_sum'] = df.groupby('user_id', group_keys=False).apply(
                lambda x: x.set_index('timestamp').rolling('1H')['amount'].sum().values
            )
            # Note: The above returns numpy array which assigns correctly if sorted
            
            # Re-calculate correct velocity and agg for the current row's context
            
            # Re-calculate correct velocity and agg for the current row's context
            # Note: The simple rolling sum looks BACKWARD. 
            # To flag the EARLIER transactions based on FUTURE behavior (in the same batch),
            # we need to group by a fixed time bucket or looking ahead.
            # However, standard fraud checks are usually retroactive. 
            # BUT, for this "Manual File Check", the user expects us to analyze the WHOLE pattern.
            
            # Strategy: Bucket by Hour (Fixed Windows) for simpler "Structure" detection
            # Round timestamp to nearest hour for grouping
            df['hour_bucket'] = df['timestamp'].dt.floor('H')
            
            # Calculate Burst Metrics (Totals within that hour bucket)
            hourly_stats = df.groupby(['user_id', 'hour_bucket']).agg(
                batch_velocity=('transaction_id', 'count'),
                batch_sum=('amount', 'sum')
            ).reset_index()
            
            # Merge back to main DF
            df = df.merge(hourly_stats, on=['user_id', 'hour_bucket'], how='left')
            
            # Advanced Fraud Logic (Row-wise, but using Batch features)
            def check_fraud_advanced(row):
                reasons = []
                is_fraud = 0
                
                # --- Rule 1: Velocity (Batch Context) ---
                # check both rolling (immediate past) and batch (whole hour context)
                if row['batch_velocity'] > 5:
                    is_fraud = 1
                    reasons.append(f"High velocity (Batch: {row['batch_velocity']} tx/hr)")

                # --- Rule 2: Structuring (Batch Context) ---
                # Lowered to 2000 to catch the 3x 980 case (2940)
                if row['batch_sum'] > 2000:
                    is_fraud = 1
                    reasons.append(f"High aggregate volume (Batch: ${row['batch_sum']:.2f}/hr)")
                
                # --- Rule 3: Card Testing ---
                # Tightened: Vel > 1 (catch simple ping pairs like 0.01, 0.01)
                if float(row['amount']) < 5.0 and row['batch_velocity'] > 1:
                    is_fraud = 1
                    reasons.append("Possible card testing")

                # --- Rule 4: High-Risk Time Windows ---
                hour = row['timestamp'].hour
                if (hour >= 23) or (hour <= 5):
                     if float(row['amount']) > 1000 or row['batch_velocity'] > 2:
                        is_fraud = 1
                        reasons.append("Suspicious activity during high-risk hours")
                
                # --- Rule 5: Max Limit Avoidance ---
                # Catch 999.99 or 980 patterns specifically
                if 900 <= float(row['amount']) < 1000:
                    is_fraud = 1
                    reasons.append("Max limit avoidance (Structuring)")

                # --- Hard Rules ---
                # Lowered to 4500 to catch the 5000 and 9000 cases
                if float(row['amount']) > 4500:
                    is_fraud = 1
                    reasons.append("High-value transaction (>$4.5k)")
                
                if is_fraud and not reasons:
                    reasons.append("Detected by advanced rules")
                    
                return is_fraud, ", ".join(list(set(reasons))) if reasons else "Normal"
            
            # Apply detection logic
            results = df.apply(check_fraud_advanced, axis=1)
            df['target_fraud'] = [x[0] for x in results]
            df['fraud_reason'] = [x[1] for x in results]
            
            # Filter Fraud
            fraud_df = df[df['target_fraud'] == 1]
            
            print(f"DEBUG: Found {len(fraud_df)} frauds out of {len(df)} transactions", flush=True)
            
            # Format for JSON
            fraud_df['timestamp'] = fraud_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Deduplicate by transaction_id if present
            if 'transaction_id' in fraud_df.columns:
                fraud_df = fraud_df.drop_duplicates(subset=['transaction_id'])
            
            # Replace NaN with None (which becomes null in JSON) to avoid "Unexpected token N" error
            fraud_df = fraud_df.where(pd.notnull(fraud_df), None)
            
            # --- FEEDBACK LOOP: SAVE DETECTIONS ---
            # Append confirmed frauds to a local CSV for the training pipeline to pick up
            try:
                feedback_file = os.path.join(DATA_DIR, 'detected_frauds.csv')
                # We only save the columns needed for training (matches historical_data schema)
                # transaction_id, user_id, amount, terminal_id, timestamp, tx_count_1h, avg_diff_ratio, is_high_risk_hr, target_fraud
                
                # Ensure columns exist (backfill if missing)
                save_cols = ['transaction_id', 'user_id', 'amount', 'timestamp', 'target_fraud']
                # Add calculated features if they exist, else default
                for col_name in ['terminal_id', 'tx_count_1h', 'avg_diff_ratio', 'is_high_risk_hr']:
                    if col_name not in fraud_df.columns:
                        fraud_df[col_name] = 0 # Default/Placeholder
                
                # Use 'batch_velocity' as 'tx_count_1h' if available because it's more accurate for this batch
                if 'batch_velocity' in fraud_df.columns:
                    fraud_df['tx_count_1h'] = fraud_df['batch_velocity']

                # Select and Rename to match training schema
                # We need: transaction_id, user_id, amount, terminal_id, timestamp, tx_count_1h, avg_diff_ratio, is_high_risk_hr, target_fraud
                feedback_df = fraud_df.copy()
                
                # Ensure is_high_risk_hr is calculated if not present
                if 'is_high_risk_hr' not in feedback_df.columns:
                     feedback_df['is_high_risk_hr'] = feedback_df['timestamp'].apply(
                         lambda x: 1 if (pd.to_datetime(x).hour >= 23 or pd.to_datetime(x).hour <= 5) else 0
                     )

                # Append to file
                hdr = not os.path.exists(feedback_file)
                feedback_df.to_csv(feedback_file, mode='a', index=False, header=hdr)
                print(f"DEBUG: Saved {len(feedback_df)} detection records to {feedback_file}", flush=True)
            except Exception as save_e:
                print(f"WARNING: Failed to save feedback data: {save_e}", flush=True)

            return jsonify(fraud_df.to_dict(orient='records'))
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error processing file: {e}")
            return jsonify({"error": str(e)}), 500
            

    return jsonify({"error": "Invalid file type. Please upload Excel or CSV."}), 400

@app.route('/api/producer/status')
def get_producer_status():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r') as f:
                status = f.read().strip()
        except:
            status = "RUNNING"
    else:
        status = "RUNNING"
    return jsonify({"status": status})

@app.route('/api/user/<int:user_id>/stats')
def get_user_statistics(user_id):
    # Check cache first
    try:
        stats = redis_cache.get_user_stats(user_id)
        if stats:
             return jsonify(stats)
    except NameError:
         pass # redis_cache not imported
    
    # Compute from data
    # In a real app we'd query a DB. Here we scan recent file.
    if os.path.exists(RECENT_FILE):
        try:
             df = pd.read_json(RECENT_FILE) if RECENT_FILE.endswith('.json') else pd.read_excel(RECENT_FILE)
             user_df = df[df['user_id'] == user_id]
             
             if not user_df.empty:
                stats = {
                    'total_transactions': len(user_df),
                    'total_amount': float(user_df['amount'].sum()),
                    'avg_amount': float(user_df['amount'].mean()),
                    'fraud_count': len(user_df[user_df.get('is_fraud', 0) == 1])
                }
                
                # Cache for future requests
                try:
                    redis_cache.cache_user_stats(user_id, stats)
                except: pass
                
                return jsonify(stats)
        except Exception:
             pass

    return jsonify({"message": "User not found in recent data"}), 404

@app.route('/api/alerts/high_priority')
def get_high_priority_alerts():
    alert_file = '/app/data/high_priority_alerts.json'
    
    if not os.path.exists(alert_file):
        return jsonify([])
    
    try:
        df = pd.read_json(alert_file)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return render_template('index.html')

import subprocess
import threading

def run_docker_compose():
    try:
        # Run docker-compose up -d in the project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # Adjust for structure: src/web_app/app.py -> project_root is two levels up? 
        # Actually app.py is in src/web_app. So dirname is src/web_app. dirname(dirname) is src. 
        # We need to go one more up to PROJECT.
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        print(f"Starting project from: {project_root}")
        subprocess.run(["docker-compose", "up", "-d"], cwd=project_root, check=True)
        print("Project started successfully.")
    except Exception as e:
        print(f"Error starting project: {e}")

@app.route('/api/start', methods=['POST'])
def start_project():
    # Run in a separate thread to not block the request
    thread = threading.Thread(target=run_docker_compose)
    thread.start()
    return jsonify({"status": "starting", "message": "Project start command initiated."})

@app.route('/api/status')
def get_service_status():
    services = {
        "airflow": False,
        "mlflow": False
    }
    import socket
    def check_socket(host, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                return s.connect_ex((host, port)) == 0
        except:
            return False

    # Check internal docker network names and ports
    services["airflow"] = check_socket('airflow-webserver', 8080)
    services["mlflow"] = check_socket('mlflow-server', 5000)
    return jsonify(services)

@app.route('/api/live')
def get_live_data():
    # Use RECENT_FILE only to avoid file locking on Windows
    target_file = RECENT_FILE
    
    if not target_file or not os.path.exists(target_file):
        return jsonify([])
        
    try:
        if target_file.endswith('.json'):
            df = pd.read_json(target_file, orient='records')
        else:
             df = pd.read_excel(target_file, engine='openpyxl')
             
        # Return last 50 transactions of ANY type
        live_df = df.tail(50)
        
        if 'timestamp' in live_df.columns:
            live_df['timestamp'] = pd.to_datetime(live_df['timestamp'])
            live_df = live_df.sort_values(by='timestamp', ascending=False)
            live_df['timestamp'] = live_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
        import json
        from flask import make_response
        
        # Replace NaN with None before serialization to avoid JSON errors
        live_df = live_df.where(pd.notnull(live_df), None)
        records = live_df.to_dict(orient='records')
        
        res = make_response(jsonify(records))
        res.headers['X-Total-Count'] = str(len(df))
        return res
    except Exception as e:
        print(f"Error reading live data from {target_file}: {e}")
        return jsonify([])

@app.route('/api/stats')
def get_stats():
    health_file = os.path.join(DATA_DIR, 'system_health.json')
    stats_file = '/shared/dashboard/dashboard_stats.json' if os.path.exists('/shared/dashboard') else os.path.join(DATA_DIR, 'dashboard_stats.json')
    
    stats = {
        "status": "UNKNOWN",
        "drift_score": 0,
        "current_mean": 0,
        "baseline_mean": 50.0,
        "timestamp": None,
        "total_processed": 0,
        "fraud_detected": 0,
        "clean_transactions": 0,
        "failed_transactions": 0
    }
    
    # Merge health data
    if os.path.exists(health_file):
        try:
            with open(health_file, 'r') as f:
                data = json.load(f)
                stats.update(data)
        except Exception as e:
            print(f"Error reading health stats: {e}")
            
    # Merge global processing stats
    if os.path.exists(stats_file):
        try:
            with open(stats_file, 'r') as f:
                d = json.load(f)
                stats["total_processed"] = d.get("total", 0)
                stats["fraud_detected"] = d.get("fraud", 0)
                stats["clean_transactions"] = d.get("clean", 0)
                stats["failed_transactions"] = d.get("failed", 0)
        except Exception as e:
            print(f"Error reading dashboard stats: {e}")
            
    return jsonify(stats)

@app.route('/api/fraud')
def get_fraud_data():
    target_file = RECENT_FILE 
    
    frauds = []
    try:
        # Read Recent JSON which contains LATEST processed data
        if os.path.exists(target_file):
            try:
                df = pd.read_json(target_file, orient='records')
                # Filter for Fraud Only
                if 'is_fraud' in df.columns:
                     fraud_df = df[df['is_fraud'] == 1].copy()
                     if not fraud_df.empty:
                         # Sort by timestamp desc
                         if 'timestamp' in fraud_df.columns:
                             fraud_df['timestamp'] = pd.to_datetime(fraud_df['timestamp'])
                             fraud_df = fraud_df.sort_values(by='timestamp', ascending=False)
                             fraud_df['timestamp'] = fraud_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                         
                         import json
                         frauds = json.loads(fraud_df.head(50).to_json(orient='records'))
            except Exception as e:
                print(f"Error filtering fraud from recent.json: {e}")
            
        return jsonify(frauds)
    except Exception as e:
        return jsonify([])
            
    except Exception as e:
        print(f"Error reading fraud data: {e}")
        return jsonify([])

# --- SHAP EXPLAINABILITY ---
import xgboost as xgb
import shap
import json
import numpy as np

# Globals for Lazy Loading
model = None
explainer = None

def get_explainer():
    global model, explainer
    model_path = '/app/models/model.json' 
    
    if model is None:
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found at {model_path}")
            
        print("Loading Model for SHAP...", flush=True)
        model = xgb.XGBClassifier()
        model.load_model(model_path)
        
        print("Initializing SHAP Explainer...", flush=True)
        explainer = shap.TreeExplainer(model)
        
    return model, explainer

@app.route('/api/explain/<tx_id>')
def explain_transaction(tx_id):
    try:
        model, explainer = get_explainer()
        
        feat_path = '/shared/dashboard/live_features.json'
        if not os.path.exists(feat_path):
            return jsonify({"error": "Feature history not found"}), 404
            
        target_row = None
        # Efficient Reverse Read Strategy for 470MB file
        
        def reverse_readline(filename, buf_size=8192):
            """A generator that returns the lines of a file in reverse order"""
            with open(filename, 'rb') as fh:
                segment = None
                offset = 0
                fh.seek(0, os.SEEK_END)
                file_size = remaining_size = fh.tell()
                while remaining_size > 0:
                    offset = min(file_size, offset + buf_size)
                    fh.seek(file_size - offset)
                    buffer = fh.read(min(remaining_size, buf_size)).decode('utf-8')
                    remaining_size -= buf_size
                    lines = buffer.split('\n')
                    if segment is not None:
                        if buffer[-1] != '\n':
                            lines[-1] += segment
                        else:
                            yield segment
                    segment = lines[0]
                    for index in range(len(lines) - 1, 0, -1):
                        if lines[index]:
                            yield lines[index]
                if segment is not None:
                    yield segment

        for line in reverse_readline(feat_path):
            try:
                row = json.loads(line)
                if row.get('transaction_id') == tx_id:
                    target_row = row
                    break # Found the most recent matching tx_id, stop scanning
            except: pass
                
        if not target_row:
             return jsonify({"error": "Transaction not found in feature logs"}), 404
             
        features_dict = {k:v for k,v in target_row.items() if k != 'transaction_id'}
        X_row = pd.DataFrame([features_dict])
        
        # XGBoost model expects exactly these 14 features in this order
        expected_features = [
            'amount', 'hour_sin', 'hour_cos', 'day_of_week', 'is_weekend', 'tx_count_1h',
            'tx_count_24h', 'amount_last_1h', 'user_amount_mean', 'user_amount_std',
            'amount_z_score', 'avg_diff_ratio', 'terminal_fraud_rate', 'amount_x_tx_count'
        ]
        
        # Ensure all expected features are present (fill missing with 0) and in correct order
        for f in expected_features:
            if f not in X_row.columns:
                X_row[f] = 0.0
                
        X_row = X_row[expected_features]
        
        # Calculate SHAP
        shap_values = explainer.shap_values(X_row)
        
        # Handle Output
        vals = shap_values[0] if isinstance(shap_values, list) else shap_values[0]
        # Check logic: binary classifier usually returns (N, Features) array.
        # If vals is array, good.
        # If shap_values is list, it is multiclass. XGBClassifier binary is usually array.
        
        base_value = explainer.expected_value
        if isinstance(base_value, list) or isinstance(base_value, np.ndarray): 
            base_value = base_value[0]
        
        explanation = []
        cols = X_row.columns
        for i, col in enumerate(cols):
            val = vals[i] if len(vals) > i else 0
            explanation.append({
                "name": col,
                "value": float(X_row.iloc[0, i]),
                "shap": float(val)
            })
            
        explanation.sort(key=lambda x: abs(x['shap']), reverse=True)
        
        return jsonify({
            "base_value": float(base_value),
            "features": explanation[:6]
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
