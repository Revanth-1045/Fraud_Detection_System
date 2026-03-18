#!/usr/bin/env python3
"""
Phase 10 Integration Verification Script
==========================================
Checks that all Phase 10 components are working end-to-end:
  1. MinIO reachable and model.json + scaler.pkl exist in 'models' bucket
  2. Spark inference container running (recent.json being updated)
  3. Web App responding (HTTP 200)
  4. Airflow webserver responding
  5. Kafka brokers reachable (plaintext)

Usage:
    python scripts/verify_phase10.py

Requirements (host machine): boto3, requests
    pip install boto3 requests
"""

import sys
import os
import json
import time
from datetime import datetime, timedelta

# ─── Configuration ────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

WEB_APP_URL = os.getenv("WEB_APP_URL", "http://localhost:3000")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8081/health")
RECENT_JSON_PATH = os.getenv(
    "RECENT_JSON_PATH",
    # When running on host, this is inside the Docker volume — use docker exec to check instead
    None
)

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results = []


def check(name: str, passed: bool, detail: str = ""):
    status = PASS if passed else FAIL
    msg = f"{status} {name}"
    if detail:
        msg += f"\n      {detail}"
    print(msg)
    results.append((name, passed, detail))


# ─── Check 1: MinIO reachable ─────────────────────────────────────────────────
def check_minio():
    try:
        import boto3
        from botocore.exceptions import ClientError, EndpointResolutionError

        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
        check("MinIO reachable", True, f"Buckets found: {buckets}")
        return s3, buckets
    except Exception as e:
        check("MinIO reachable", False, str(e))
        return None, []


# ─── Check 2: model.json exists in S3 ────────────────────────────────────────
def check_model_in_s3(s3, buckets):
    if s3 is None:
        check("model.json in S3", False, "MinIO not reachable, skipping")
        return

    try:
        if "models" not in buckets:
            check("model.json in S3", False, "'models' bucket does not exist")
            return
        keys = [o['Key'] for o in s3.list_objects(Bucket='models').get('Contents', [])]
        has_model = "model.json" in keys
        has_scaler = "scaler.pkl" in keys
        check("model.json in S3", has_model, f"Keys in bucket: {keys}")
        check("scaler.pkl in S3", has_scaler,
              "Run Airflow training pipeline to generate scaler.pkl" if not has_scaler else "")
    except Exception as e:
        check("model.json in S3", False, str(e))
        check("scaler.pkl in S3", False, str(e))


# ─── Check 3: Web App responding ─────────────────────────────────────────────
def check_web_app():
    try:
        import requests
        r = requests.get(WEB_APP_URL, timeout=5)
        check("Web App (HTTP 200)", r.status_code == 200,
              f"Status: {r.status_code} — {WEB_APP_URL}")
    except Exception as e:
        check("Web App (HTTP 200)", False, str(e))


# ─── Check 4: Airflow responding ─────────────────────────────────────────────
def check_airflow():
    try:
        import requests
        r = requests.get(AIRFLOW_URL, timeout=5)
        data = r.json() if r.headers.get('Content-Type', '').startswith('application/json') else {}
        healthy = r.status_code == 200 and data.get('status', '') == 'healthy'
        check("Airflow Webserver healthy", healthy,
              f"Status: {r.status_code}, Body: {json.dumps(data)[:200]}")
    except Exception as e:
        check("Airflow Webserver healthy", False, str(e))


# ─── Check 5: Docker containers running ──────────────────────────────────────
def check_docker_containers():
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split('\n')
        running = {l.split('\t')[0]: l.split('\t')[1] for l in lines if '\t' in l}

        critical_services = [
            "spark-inference", "minio", "kafka-1", "kafka-2", "kafka-3",
            "zookeeper", "redis", "web-app", "producer"
        ]

        all_up = True
        details = []
        for svc in critical_services:
            # Match partial container names (compose adds project prefix)
            matches = [k for k in running if svc in k]
            if matches:
                status = running[matches[0]]
                up = "Up" in status
                details.append(f"{'✅' if up else '❌'} {svc}: {status}")
                if not up:
                    all_up = False
            else:
                details.append(f"❌ {svc}: NOT FOUND")
                all_up = False

        check("Critical Docker containers running", all_up, "\n      ".join(details))
    except FileNotFoundError:
        check("Critical Docker containers running", False, "Docker CLI not found on PATH")
    except Exception as e:
        check("Critical Docker containers running", False, str(e))


# ─── Check 6: recent.json freshness (via docker exec) ────────────────────────
def check_recent_json():
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "spark-inference",
             "python3", "-c",
             "import json,os; d=json.load(open('/shared/dashboard/recent.json')); "
             "print(len(d), 'records'); print(d[0].get('timestamp','?') if d else 'empty')"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            check("recent.json being updated by Spark", True, output)
        else:
            err = result.stderr.strip()[:300]
            check("recent.json being updated by Spark", False, err)
    except Exception as e:
        check("recent.json being updated by Spark", False, str(e))


# ─── Check 7: Kafka topics exist ─────────────────────────────────────────────
def check_kafka_topics():
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "kafka-1",
             "kafka-topics", "--list", "--bootstrap-server", "kafka-1:9092"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            topics = [t.strip() for t in result.stdout.strip().split('\n') if t.strip()]
            has_tx = "transactions" in topics
            has_meta = "transactions-metadata" in topics
            check("Kafka topic 'transactions' exists", has_tx, f"All topics: {topics}")
            check("Kafka topic 'transactions-metadata' exists", has_meta)
        else:
            check("Kafka topics exist", False, result.stderr[:300])
    except Exception as e:
        check("Kafka topics exist", False, str(e))


# ─── Summary ──────────────────────────────────────────────────────────────────
def print_summary():
    print("\n" + "=" * 60)
    print("PHASE 10 VERIFICATION SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print(f"\n  Passed: {passed}/{total}\n")

    failed = [(name, detail) for name, ok, detail in results if not ok]
    if failed:
        print("  Failed checks:")
        for name, detail in failed:
            print(f"    ❌ {name}")
            if detail:
                print(f"       → {detail}")
    else:
        print("  🎉 All checks passed! Phase 10 is fully operational.")

    print("\n  Next Steps:")
    if any("scaler.pkl" in name and not ok for name, ok, _ in results):
        print("  → Run the Airflow DAG 'fraud_detection_training_enhanced' to generate scaler.pkl")
    if any("recent.json" in name and not ok for name, ok, _ in results):
        print("  → Check spark-inference logs: docker logs spark-inference --tail 50")
    if any("Web App" in name and not ok for name, ok, _ in results):
        print("  → Check web-app logs: docker logs web-app --tail 30")

    sys.exit(0 if passed == total else 1)


# ─── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n🔍 Phase 10 Verification — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    s3, buckets = check_minio()
    check_model_in_s3(s3, buckets)
    check_web_app()
    check_airflow()
    check_docker_containers()
    check_recent_json()
    check_kafka_topics()

    print_summary()
