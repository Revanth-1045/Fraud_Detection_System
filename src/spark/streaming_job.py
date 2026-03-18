from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, count, mean, window, hour, lit, udf, last, max, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import mlflow.xgboost
import xgboost as xgb
import pandas as pd
import os
import logging
import time
import random
import json
import pickle
from datetime import datetime
import redis  # Phase 3: Redis for RAM fetch
import boto3

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
MODEL_PATH = "/opt/spark/models/model.json"
SCALER_PATH = "/opt/spark/models/scaler.pkl"

# Phase 10: Shadow/Canary Mode
SHADOW_MODE = os.getenv("SHADOW_MODE", "false").lower() == "true"
if SHADOW_MODE:
    logger.info("🟡 SHADOW MODE ENABLED: Predictions logged but all transactions ALLOWED")


def download_model_artifacts():
    """
    Phase 10: Download model (and scaler) from S3/MinIO with retry loop.
    Called ONCE at startup before SparkSession is created.
    Retries up to 10 times with 15s back-off.
    """
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not (s3_endpoint and aws_key and aws_secret):
        logger.warning("⚠️ S3 credentials not set. Skipping S3 model download.")
        return

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)

    max_retries = 10
    retry_delay = 15  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}/{max_retries}] Connecting to S3 at {s3_endpoint}...")
            s3 = boto3.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret,
            )
            # --- Download model.json ---
            s3.download_file("models", "model.json", MODEL_PATH)
            logger.info("✅ model.json downloaded successfully.")

            # --- Download scaler.pkl (optional — may not exist yet) ---
            try:
                s3.download_file("models", "scaler.pkl", SCALER_PATH)
                logger.info("✅ scaler.pkl downloaded successfully.")
            except Exception:
                logger.info("ℹ️ scaler.pkl not found in S3 (will use raw Amount values).")

            return  # Success — exit retry loop

        except Exception as e:
            logger.warning(f"⚠️ Download failed: {e}")
            if attempt < max_retries:
                logger.info(f"   Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ All retries exhausted. Model download failed. Will use pre-existing model if available.")


def get_spark_session():
    return SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.worker.dir", "/tmp/spark/worker") \
        .config("spark.sql.warehouse.dir", "/tmp/spark/warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoint") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()


class ModelWrapper:
    model = None
    scaler = None

    @classmethod
    def get_model(cls):
        if cls.model is None:
            if os.path.exists(MODEL_PATH):
                try:
                    logger.info(f"Loading model from {MODEL_PATH}...")
                    cls.model = xgb.XGBClassifier()
                    cls.model.load_model(MODEL_PATH)
                    logger.info("✅ Model loaded.")
                except Exception as e:
                    logger.error(f"Error loading model: {e}")
            else:
                logger.warning(f"Model not found at {MODEL_PATH}")
        return cls.model

    @classmethod
    def get_scaler(cls):
        if cls.scaler is None:
            if os.path.exists(SCALER_PATH):
                try:
                    with open(SCALER_PATH, 'rb') as f:
                        cls.scaler = pickle.load(f)
                    logger.info("✅ RobustScaler loaded.")
                except Exception as e:
                    logger.warning(f"Could not load scaler: {e}")
        return cls.scaler


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Broadcast User Averages (Mocked for demo)
    try:
        hist_df = pd.read_csv("/app/data/transactions.csv")
        user_avgs = hist_df.groupby("user_id")["amount"].mean().to_dict()
        logger.info(f"Loaded profiles for {len(user_avgs)} users.")
    except Exception as e:
        logger.warning(f"Could not load historical data: {e}. Using defaults.")
        user_avgs = {i: 100.0 for i in range(1, 1001)}
    broadcast_avgs = spark.sparkContext.broadcast(user_avgs)

    # Schema (V1-V28 + metadata)
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("terminal_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("tx_count_1h", IntegerType()),
        StructField("avg_diff_ratio", DoubleType()),
        StructField("is_high_risk_hr", IntegerType()),
        StructField("target_fraud", IntegerType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("ip_address", StringType()),
        StructField("device_id", StringType()),
        StructField("merchant_id", StringType())
    ] + [StructField(f"V{i}", DoubleType()) for i in range(1, 29)])

    # SSL Configuration
    is_ssl = "9093" in KAFKA_BOOTSTRAP_SERVERS

    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "transactions-metadata") \
        .option("startingOffsets", "latest")

    if is_ssl:
        reader = reader \
            .option("kafka.security.protocol", "SSL") \
            .option("kafka.ssl.truststore.location", "/etc/kafka/secrets/kafka.server.truststore.jks") \
            .option("kafka.ssl.truststore.password", "kafka-secret") \
            .option("kafka.ssl.keystore.location", "/etc/kafka/secrets/kafka.server.keystore.jks") \
            .option("kafka.ssl.keystore.password", "kafka-secret")

    df_kafka = reader.load()

    # Parse Metadata
    metadata_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", IntegerType())
    ])

    # Phase 3: Hydrate from Redis RAM
    @udf(returnType=StringType())
    def fetch_from_redis(tx_id):
        if not tx_id:
            return None
        try:
            r = redis.Redis(host='redis-ram-cache', port=6379, decode_responses=True)
            data = r.get(f"tx:{tx_id}")
            return data
        except Exception:
            return None

    df_meta = df_kafka.select(from_json(col("value").cast("string"), metadata_schema).alias("meta")).select("meta.*")
    df_json = df_meta.withColumn("json_data", fetch_from_redis(col("transaction_id")))
    df_json = df_json.filter(col("json_data").isNotNull())
    df_parsed = df_json.select(from_json(col("json_data"), schema).alias("data")).select("data.*")
    df_parsed = df_parsed.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Feature Engineering in Spark
    df_enriched = df_parsed.withColumn("calc_high_risk_hr",
        ((hour(col("timestamp")) >= 1) & (hour(col("timestamp")) <= 5)).cast("integer")
    )

    # Window Function for tx_count_1h + amount_last_1h
    df_windowed = df_enriched \
        .withWatermark("timestamp", "1 minutes") \
        .groupBy(
            window(col("timestamp"), "1 hour", "1 minute"),
            col("user_id")
        ) \
        .agg(
            count("*").alias("calc_tx_count_1h"),
            spark_sum("amount").alias("calc_amount_last_1h"),
            last("amount").alias("amount"),
            last("transaction_id").alias("transaction_id"),
            last("calc_high_risk_hr").alias("is_high_risk_hr"),
            last("timestamp").alias("timestamp"),
            last("tx_count_1h").alias("tx_count_1h"),
            last("avg_diff_ratio").alias("avg_diff_ratio"),
            last("lat").alias("lat"),
            last("lon").alias("lon"),
            last("ip_address").alias("ip_address"),
            last("device_id").alias("device_id"),
            last("merchant_id").alias("merchant_id"),
            *[last(f"V{i}").alias(f"V{i}") for i in range(1, 29)]
        )

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records (windows)")

        pdf = batch_df.toPandas()

        # --- FRAUD RING DETECTION ---
        pdf['is_ring_fraud'] = 0
        pdf['ring_reason'] = ""

        if 'device_id' in pdf.columns:
            dev_counts = pdf.groupby('device_id')['user_id'].nunique()
            bad_devs = dev_counts[dev_counts > 1].index.tolist()
            if bad_devs:
                mask = pdf['device_id'].isin(bad_devs)
                pdf.loc[mask, 'is_ring_fraud'] = 1
                pdf.loc[mask, 'ring_reason'] = "Shared Device"

        if 'ip_address' in pdf.columns:
            ip_counts = pdf.groupby('ip_address')['user_id'].nunique()
            bad_ips = ip_counts[ip_counts > 1].index.tolist()
            if bad_ips:
                mask = pdf['ip_address'].isin(bad_ips)
                pdf.loc[mask, 'is_ring_fraud'] = 1
                pdf.loc[mask, 'ring_reason'] = pdf.loc[mask, 'ring_reason'].apply(
                    lambda x: x + ("; " if x else "") + "Shared IP"
                )

        # --- MERCHANT ANALYTICS ---
        if 'merchant_id' in pdf.columns:
            mask_risky = pdf['merchant_id'].str.startswith('M_00', na=False)
            if mask_risky.any():
                pdf.loc[mask_risky, 'is_ring_fraud'] = 1
                pdf.loc[mask_risky, 'ring_reason'] = pdf.loc[mask_risky, 'ring_reason'].apply(
                    lambda x: x + ("; " if x else "") + "High Risk Merchant"
                )

        # --- MLOps DRIFT DETECTION (Phase 9) ---
        BASELINE_MEAN = 50.0
        current_mean = pdf['amount'].mean()
        drift_score = abs(current_mean - BASELINE_MEAN) / BASELINE_MEAN if BASELINE_MEAN > 0 else 0

        health_status = "OK"
        if drift_score > 0.5:
            health_status = "DRIFT_WARNING"
        if drift_score > 1.0:
            health_status = "DRIFT_CRITICAL"

        health_data = {
            "timestamp": datetime.now().isoformat(),
            "batch_id": batch_id,
            "drift_score": drift_score,
            "current_mean": current_mean,
            "status": health_status,
            "baseline_mean": BASELINE_MEAN
        }

        health_path = "/tmp/system_health.json"
        try:
            with open(health_path, 'w') as f:
                json.dump(health_data, f)
        except Exception as e:
            logger.error(f"Failed to write system_health.json: {e}")

        # --- Advanced Feature Engineering ---
        import numpy as np

        pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
        pdf['hour'] = pdf['timestamp'].dt.hour
        pdf['day_of_week'] = pdf['timestamp'].dt.dayofweek
        pdf['is_weekend'] = pdf['day_of_week'].isin([5, 6]).astype(int)
        pdf['hour_sin'] = np.sin(2 * np.pi * pdf['hour'] / 24)
        pdf['hour_cos'] = np.cos(2 * np.pi * pdf['hour'] / 24)

        # Statistical Features
        avgs = broadcast_avgs.value
        pdf["user_amount_mean"] = pdf["user_id"].map(lambda u: avgs.get(u, 100.0))
        pdf["user_amount_std"] = pdf["user_amount_mean"] * 0.5
        pdf['amount_z_score'] = (pdf['amount'] - pdf["user_amount_mean"]) / (pdf["user_amount_std"] + 1e-6)
        pdf['avg_diff_ratio'] = pdf['amount'] / pdf["user_amount_mean"]

        # Velocity
        if "calc_tx_count_1h" in pdf.columns:
            pdf["tx_count_1h"] = pdf["calc_tx_count_1h"]
        if "calc_amount_last_1h" in pdf.columns:
            pdf["amount_last_1h"] = pdf["calc_amount_last_1h"]
        else:
            pdf["amount_last_1h"] = pdf["amount"] * pdf["tx_count_1h"]

        pdf["tx_count_24h"] = pdf["tx_count_1h"] * 24
        pdf["terminal_fraud_rate"] = 0.05
        pdf['amount_x_tx_count'] = pdf['amount'] * pdf['tx_count_1h']

        # Geolocation
        import math
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371
            dLat = math.radians(lat2 - lat1)
            dLon = math.radians(lon2 - lon1)
            a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        US_CENTER_LAT, US_CENTER_LON = 39.8, -98.6
        if 'lat' in pdf.columns and 'lon' in pdf.columns:
            pdf['lat'] = pdf['lat'].fillna(US_CENTER_LAT)
            pdf['lon'] = pdf['lon'].fillna(US_CENTER_LON)
            pdf['distance_from_center'] = pdf.apply(
                lambda row: haversine(row['lat'], row['lon'], US_CENTER_LAT, US_CENTER_LON), axis=1
            )
        else:
            pdf['distance_from_center'] = 0.0

        # --- Feature Selection (must match training) ---
        features = [
            'amount', 'hour_sin', 'hour_cos', 'day_of_week', 'is_weekend',
            'tx_count_1h', 'tx_count_24h', 'amount_last_1h', 'user_amount_mean',
            'user_amount_std', 'amount_z_score', 'avg_diff_ratio',
            'terminal_fraud_rate', 'amount_x_tx_count'
        ]
        for f in features:
            if f not in pdf.columns:
                pdf[f] = 0

        X = pdf[features].copy()

        # Phase 10: Apply RobustScaler to Amount if available
        logger.info(f"DEBUG AMOUNT - Before scaling: {X['amount'].head().tolist()}")
        scaler = ModelWrapper.get_scaler()
        if scaler is not None:
            try:
                X["raw_amount"] = X["amount"]
                logger.info(f"DEBUG AMOUNT - raw_amount copied: {X['raw_amount'].head().tolist()}")
                X[["amount"]] = scaler.transform(X[["amount"]])
                logger.info(f"DEBUG AMOUNT - After scaling: {X['amount'].head().tolist()}")
            except Exception as e:
                logger.warning(f"Scaler transform failed: {e}")
                X["raw_amount"] = X["amount"]
        else:
            X["raw_amount"] = X["amount"]

        # --- FEATURE EXPORT FOR SHAP ---
        try:
            export_df = X.copy()
            export_df['transaction_id'] = pdf['transaction_id'].values
            feat_file = "/shared/dashboard/live_features.json"
            
            # Keep only the last 1000 records to prevent memory bloat in the RAM volume
            if os.path.exists(feat_file):
                try:
                    existing_df = pd.read_json(feat_file, orient='records', lines=True)
                    combined_df = pd.concat([existing_df, export_df], ignore_index=True)
                    if 'transaction_id' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(subset=['transaction_id'], keep='last')
                    export_df = combined_df.tail(1000)
                except Exception:
                    pass
            
            # Write completely rather than append, since we truncated
            export_df.to_json(feat_file, orient='records', lines=True)
            logger.info(f"✅ Updated {feat_file} ({len(export_df)} features).")
        except Exception as e:
            logger.error(f"Failed to export features: {e}")

        # --- INFERENCE ---
        model = ModelWrapper.get_model()
        if model:
            try:
                X_infer = X.drop(columns=["raw_amount"]) if "raw_amount" in X.columns else X
                preds = model.predict(X_infer)
                pdf["model_prediction"] = preds

                # Increased threshold to practically eliminate false 'Impossible Travel' flags for international coordinates
                pdf["is_impossible_travel"] = pdf["distance_from_center"] > 20000

                # Phase 10: Shadow Mode — log prediction but allow all
                logger.info(f"DEBUG INFER: Shadow mode is {SHADOW_MODE}")
                if SHADOW_MODE:
                    pdf["is_fraud"] = 0
                    pdf["fraud_reason"] = pdf.apply(
                        lambda row: ("SHADOW:" + (
                            "Model" if row["model_prediction"] == 1 else ""
                        ) + ("|Travel" if row["is_impossible_travel"] else "")
                        + ("|Ring" if row["is_ring_fraud"] == 1 else "")).rstrip(":"),
                        axis=1
                    )
                    logger.info(f"🟡 SHADOW batch {batch_id}: {(preds == 1).sum()} would-be frauds suppressed.")
                else:
                    pdf["is_fraud"] = (
                        (pdf["model_prediction"] == 1) |
                        (pdf["is_impossible_travel"]) |
                        (pdf["is_ring_fraud"] == 1)
                    ).astype(int)

                    def get_reason(row):
                        try:
                            reasons = []
                            if row["model_prediction"] == 1:
                                reasons.append("Model Flagged")
                            if row["is_impossible_travel"]:
                                reasons.append(f"Impossible Travel ({int(row['distance_from_center'])}km)")
                            if row["is_ring_fraud"] == 1:
                                reasons.append(f"Entity Risk ({row['ring_reason']})")
                            if (row.get("merchant_id") or "").startswith("M_00"):
                                reasons.append("High Risk Merchant")
                            return ", ".join(reasons) if reasons else ""
                        except Exception as apply_err:
                            logger.error(f"DEBUG APPLY ERR: {apply_err}")
                            return "Error"

                    pdf["fraud_reason"] = pdf.apply(get_reason, axis=1)
                    logger.info("DEBUG INFER: Apply get_reason complete.")

                frauds = pdf[pdf["is_fraud"] == 1]
                if not frauds.empty:
                    logger.warning(f"🚨 FRAUD DETECTED: {len(frauds)} transactions!")
                    high_priority = frauds[frauds['amount'] > 50000]
                    if not high_priority.empty:
                        alert_path = "/shared/dashboard/high_priority_alerts.json"
                        try:
                            if os.path.exists(alert_path):
                                existing = pd.read_json(alert_path, orient='records')
                                alerts = pd.concat([existing, high_priority], ignore_index=True)
                            else:
                                alerts = high_priority
                            if 'transaction_id' in alerts.columns:
                                alerts = alerts.drop_duplicates(subset=['transaction_id'])
                            alerts.tail(1000).to_json(alert_path, orient='records')
                        except Exception:
                            pass

                report_cols = ["transaction_id", "user_id", "amount", "timestamp", "is_fraud",
                               "fraud_reason", "lat", "lon", "ip_address", "distance_from_center"]
                for c in report_cols:
                    if c not in pdf.columns:
                        pdf[c] = None

                report_df = pdf[report_cols].copy()
                
                # Recover original amount before scaling
                if "raw_amount" in X.columns:
                    report_df["amount"] = X["raw_amount"].values
                else:
                    logger.warning("DEBUG AMOUNT - raw_amount NOT IN X.columns!")
                
                logger.info(f"DEBUG AMOUNT - report_df after restore: {report_df['amount'].head().tolist()}")

                # Update global stats
                stats_file = "/shared/dashboard/dashboard_stats.json"
                try:
                    os.makedirs("/shared/dashboard", exist_ok=True)
                    stats = {"total": 0, "fraud": 0, "failed": 0, "clean": 0}
                    if os.path.exists(stats_file):
                        try:
                            with open(stats_file, "r") as f:
                                stats = json.load(f)
                        except Exception: pass
                    fraud_added = len(frauds)
                    total_added = len(report_df)
                    stats["total"] += total_added
                    stats["fraud"] += fraud_added
                    stats["clean"] += (total_added - fraud_added)
                    with open(stats_file, "w") as f:
                        json.dump(stats, f)
                except Exception as stats_e:
                    logger.error(f"Error updating stats: {stats_e}")

                # Write recent.json to shared RAM volume (atomic)
                json_path = "/shared/dashboard/recent.json"
                logger.info(f"DEBUG: Writing {json_path}. Batch size: {len(report_df)}")
                try:
                    recent_df = report_df
                    if os.path.exists(json_path):
                        try:
                            existing_df = pd.read_json(json_path, orient='records')
                            recent_df = pd.concat([existing_df, report_df], ignore_index=True)
                        except Exception as read_e:
                            logger.warning(f"Could not read existing recent.json: {read_e}")

                    if 'transaction_id' in recent_df.columns:
                        recent_df = recent_df.drop_duplicates(subset=['transaction_id'], keep='last')
                    if 'timestamp' in recent_df.columns:
                        recent_df = recent_df.sort_values(by='timestamp', ascending=False)

                    recent_df = recent_df.head(100)
                    if 'timestamp' in recent_df.columns:
                        recent_df['timestamp'] = recent_df['timestamp'].astype(str)

                    json_path_tmp = json_path + ".tmp"
                    recent_df.to_json(json_path_tmp, orient='records')
                    os.replace(json_path_tmp, json_path)
                    logger.info(f"✅ Updated {json_path} ({len(recent_df)} records).")

                except Exception as json_e:
                    logger.error(f"Error writing recent.json: {json_e}")

            except Exception as e:
                logger.error(f"Prediction error: {e}")

            # Phase 3 Cleanup: Purge processed Redis keys
            try:
                if 'transaction_id' in pdf.columns:
                    tx_ids = pdf['transaction_id'].tolist()
                    if tx_ids:
                        r = redis.Redis(host='redis-ram-cache', port=6379)
                        keys_to_delete = [f"tx:{id}" for id in tx_ids]
                        if keys_to_delete:
                            r.delete(*keys_to_delete)
                        logger.info(f"Purged {len(keys_to_delete)} transactions from Redis RAM.")
            except Exception as cleanup_e:
                logger.warning(f"Failed to purge Redis keys: {cleanup_e}")

            # === FEEDBACK LOOP: write scored rows to PostgreSQL ===
            # Airflow fetch_data reads this table on the next daily retraining run.
            # Wrapped in try/except — inference continues even if postgres is unavailable.
            try:
                import psycopg2
                import json as _json
                pg_conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'postgres'),
                    dbname=os.getenv('POSTGRES_DB', 'airflow'),
                    user=os.getenv('POSTGRES_USER', 'postgres'),
                    password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
                    connect_timeout=3
                )
                cur = pg_conn.cursor()
                feature_cols = ["amount", "hour_sin", "hour_cos", "is_weekend",
                                "tx_count_1h", "amount_last_1h", "tx_count_24h",
                                "amount_z_score", "avg_diff_ratio"] + [f"V{i}" for i in range(1, 29)]
                inserted = 0
                for _, row in pdf.iterrows():
                    feat_snap = {c: float(row[c]) if c in row and pd.notna(row[c]) else 0.0
                                 for c in feature_cols}
                    cur.execute("""
                        INSERT INTO feedback_labels
                          (transaction_id, amount, is_fraud, fraud_score, features_json)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                    """, (
                        str(row.get("transaction_id", "")),
                        float(row.get("amount", 0)),
                        int(row.get("is_fraud", 0)),
                        float(row.get("model_prediction", row.get("is_fraud", 0))),
                        _json.dumps(feat_snap)
                    ))
                    inserted += cur.rowcount
                pg_conn.commit()
                cur.close()
                pg_conn.close()
                logger.info(f"📥 Feedback: {inserted}/{len(pdf)} rows saved to PostgreSQL.")
            except Exception as _fb_err:
                logger.warning(f"Feedback write skipped: {_fb_err}")

        else:
            logger.info("⚠️ Model not loaded yet — skipping inference for this batch.")

    query = df_windowed.writeStream \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    try:
        # Phase 10: Download model artifacts BEFORE starting Spark
        download_model_artifacts()
        main()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        logger.info("Keeping container alive for debugging...")
        while True:
            time.sleep(60)
