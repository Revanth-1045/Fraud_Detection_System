# 🔄 Feedback Loop — Live Retraining Implementation

This document explains how the model will retrain on real transaction data collected by Spark,
instead of only using synthetic data — and how to scale it across 4 laptops using Kubernetes.

---

## The Goal

```
Today (without feedback loop):
  Synthetic Data  ──► Train  ──► Model

Tomorrow (with feedback loop):
  Synthetic Data  ─┐
                   ├──► Merge ──► Train ──► Better Model
  Live Scored Txns ─┘
```

Every day, the model gets smarter by learning from the transactions it scored yesterday.

---

## How Data Flows

```
1. Kafka produces a transaction
        │
        ▼
2. Spark scores it  ──► is_fraud = 1 / 0
        │
        ▼
3. Spark writes the result to PostgreSQL   ← NEW
   (transaction_id, amount, is_fraud, features, timestamp)
        │
        ▼
4. Airflow DAG runs at midnight
        │
        ▼
5. fetch_data reads last 7 days from PostgreSQL   ← MODIFIED
        │
        ▼
6. Real rows merged with synthetic data
        │
        ▼
7. SMOTE + training runs on combined dataset
        │
        ▼
8. New model saved to S3  ──► Spark picks it up next batch
```

---

## What Gets Changed

### 1. New PostgreSQL Table — `feedback_labels`

```sql
CREATE TABLE feedback_labels (
    transaction_id  TEXT PRIMARY KEY,
    amount          FLOAT,
    is_fraud        SMALLINT,      -- model verdict
    fraud_score     FLOAT,         -- raw probability
    features_json   JSONB,         -- feature snapshot at scoring time
    confirmed_fraud SMALLINT,      -- NULL until manually confirmed
    scored_at       TIMESTAMPTZ DEFAULT NOW()
);
```

### 2. Spark → PostgreSQL Write (after inference)

After Spark decides `is_fraud = 1 or 0`, it saves that row to PostgreSQL.
If PostgreSQL is down, Spark logs a warning and **continues normally** — no crash.

### 3. Airflow `fetch_data` — Merge Real + Synthetic

```
fetch_data()
   ├── generate 10,000 synthetic transactions   (existing)
   ├── SELECT last 7 days from feedback_labels  (new)
   └── pd.concat([synthetic, live]).drop_duplicates()
```

Log output example:
```
✅ Feedback loop: merged 2,450 live rows
   (synthetic=10000, total=12246, live_fraud=123)
```

### 4. Spark Dockerfile — one extra dependency

```dockerfile
pip install psycopg2-binary   # ← added
```

---

## Files Changed Summary

| File | Change | Risk |
|------|--------|------|
| `scripts/init_feedback.sql` | **NEW** — run once to create table | None |
| `src/spark/streaming_job.py` | +40 lines in `process_batch` | Zero — try/except |
| `src/airflow/dags/fraud_detection_training_enhanced.py` | Replace S3 stub with PG read | Zero — try/except |
| `src/spark/Dockerfile` | +1 pip dependency | Requires image rebuild |

---

## Activation Steps (when ready to enable)

```bash
# Step 1 — Create the table
docker exec -i <postgres-container> psql -U airflow -d frauddb < scripts/init_feedback.sql

# Step 2 — Rebuild Spark image (needed for psycopg2)
docker compose build spark-inference
docker compose up -d spark-inference

# Step 3 — Verify rows are being saved after a few minutes
docker exec -i <postgres-container> psql -U airflow -d frauddb \
  -c "SELECT COUNT(*), SUM(is_fraud) FROM feedback_labels;"

# Step 4 — Trigger the Airflow DAG and watch fetch_data logs
```

---

## Hourly vs Daily Training — Does More Data Change the Answer?

The short answer: **yes, more data changes the equation — but daily still wins until very large scale.**

### The Dataset Size Breakpoints

| Dataset Size | Fraud Samples/Hour | Hourly Viable? | Recommended |
|-------------|-------------------|----------------|-------------|
| 10K rows (current) | ~8 | ❌ Too few for SMOTE | `@daily` |
| 100K rows | ~80 | ❌ Unstable — high variance | `@daily` |
| 1M rows | ~800 | ⚠️ Possible but risky | `@daily` |
| 5M rows | ~4,000 | ✅ Sufficient | `@hourly` viable |
| 10M+ rows | ~8,000+ | ✅ Yes | `@hourly` or micro-batch |

### Why Hourly Fails at Small Scale

Even with more data, **hourly training has structural problems**:

1. **Catastrophic forgetting** — model trained on the last hour forgets patterns from last week
   (a fraudster who stops for 3 hours then resumes causes the model to miss them)

2. **Optuna overhead** — 20 tuning trials × 3 models = ~12 minutes of compute per run.
   Running this 24× a day = 288 minutes = 5 hours of training daily

3. **Threshold instability** — optimal threshold jumps between 0.10 and 0.45 run-to-run,
   giving Spark a different cutoff every hour

4. **Model churn** — Spark downloads a new model every hour. If training and download
   overlap, one batch gets scored with the old model mid-switch (race condition)

### When Hourly Wins

At **5M+ rows with Kubernetes + 4 nodes**:
- Each hour collects ~4,000 fraud samples (SMOTE has plenty to work with)
- `n_tuning_trials` can be reduced to 5 (fast, not exhaustive)
- Distributed XGBoost trains in ~2 minutes instead of 12
- Model churn is managed by blue/green deployment (see below)

**Verdict: with 4 laptops and 5M+ rows, switch to `@hourly`.**

---

## Connecting 4 Laptops — Kubernetes Setup

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  Kubernetes Cluster                  │
│                                                      │
│  Laptop 1 (Master)     Laptop 2, 3, 4 (Workers)     │
│  ┌───────────────┐     ┌──────┐ ┌──────┐ ┌──────┐   │
│  │ Airflow       │     │ Pod  │ │ Pod  │ │ Pod  │   │
│  │ Scheduler     │     │ XGB  │ │ LGB  │ │ CB   │   │
│  │ MinIO (S3)    │     │ train│ │ train│ │ train│   │
│  │ PostgreSQL    │     └──────┘ └──────┘ └──────┘   │
│  │ Kafka         │                                   │
│  └───────────────┘     Spark Workers (one per node)  │
│                        ┌──────┐ ┌──────┐ ┌──────┐   │
│                        │Spark │ │Spark │ │Spark │   │
│                        │Worker│ │Worker│ │Worker│   │
│                        └──────┘ └──────┘ └──────┘   │
└─────────────────────────────────────────────────────┘
```

### Step-by-Step Setup

#### Step 1 — Install Kubernetes on All 4 Laptops

On every laptop, install `k3s` (lightweight Kubernetes — works on any laptop):

```bash
# Laptop 1 (master) — run this first
curl -sfL https://get.k3s.io | sh -

# Get the token to join workers
cat /var/lib/rancher/k3s/server/node-token

# Laptop 2, 3, 4 (workers) — replace TOKEN and MASTER_IP
curl -sfL https://get.k3s.io | K3S_URL=https://<MASTER_IP>:6443 \
  K3S_TOKEN=<TOKEN> sh -
```

After this, all 4 laptops appear as nodes in the cluster:
```bash
kubectl get nodes
# NAME        STATUS   ROLES    AGE
# laptop-1    Ready    master    5m
# laptop-2    Ready    worker    3m
# laptop-3    Ready    worker    3m
# laptop-4    Ready    worker    3m
```

#### Step 2 — Switch Airflow to KubernetesExecutor

In `docker-compose.yml` or `airflow.cfg`, change the executor:

```yaml
# Before
AIRFLOW__CORE__EXECUTOR: CeleryExecutor

# After
AIRFLOW__CORE__EXECUTOR: KubernetesExecutor
```

**What this means:** Instead of one Celery worker running all tasks sequentially,
each Airflow task (`train_xgboost`, `train_lightgbm`, `train_catboost`) becomes
its own **Kubernetes pod** that can land on any of the 4 laptops.

```
Before (1 laptop):          After (4 laptops):
train_xgboost ──┐           train_xgboost → Laptop 2 ─┐
train_lightgbm ─┤ serial    train_lightgbm → Laptop 3 ─┼── TRUE parallel
train_catboost ─┘           train_catboost → Laptop 4 ─┘
Total: ~36 min              Total: ~12 min
```

#### Step 3 — Spark in Cluster Mode

Change `streaming_job.py` SparkSession config:

```python
# Before (local mode)
SparkSession.builder.appName("FraudDetection")

# After (K8s cluster)
SparkSession.builder \
  .appName("FraudDetection") \
  .master("k8s://https://<MASTER_IP>:6443") \
  .config("spark.executor.instances", "3") \       # one per worker laptop
  .config("spark.kubernetes.container.image", "fraud-spark:latest")
```

Spark automatically distributes each batch across all 4 laptops.

#### Step 4 — Distributed XGBoost Training (for 5M+ rows)

Replace the XGBoost training with Dask-distributed:

```python
import dask.distributed as dd
client = dd.Client("tcp://<MASTER_IP>:8786")  # Dask scheduler on master

import xgboost as xgb
dtrain = xgb.dask.DaskDMatrix(client, X_train, y_train)
model = xgb.dask.train(client, params, dtrain)
# Data split across 4 laptops → 4× faster
```

#### Step 5 — Shared Storage (MinIO stays the same)

MinIO already acts as S3 — all 4 nodes read/write to the same bucket.
No change needed. The master laptop hosts MinIO and PostgreSQL.

### Performance After K8s Setup

| Metric | Before (1 laptop) | After (4 laptops K8s) |
|--------|-------------------|-----------------------|
| Training time | ~36 min | ~10 min |
| Max dataset size | ~500K rows | 10M+ rows |
| Spark throughput | ~100 tx/sec | ~400 tx/sec |
| Recommended schedule | `@daily` | `@hourly` (at 5M+ rows) |

---

## Recommended Rollout Path

```
Phase 1 (Now)      → Feedback loop live,       @daily,   1 laptop,    10K rows
Phase 2 (Month 1)  → Feedback accumulates,     @daily,   1 laptop,    100K rows
Phase 3 (Month 2)  → k3s cluster setup,        @daily,   4 laptops,   500K rows
Phase 4 (Month 3)  → Dask XGBoost, 5M rows,   @hourly,  4 laptops,   5M rows
```
