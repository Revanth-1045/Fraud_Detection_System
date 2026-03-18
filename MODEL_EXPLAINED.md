# 🧠 How the Fraud Detection Model Works

This document explains, in plain English, how the system trains itself and makes fraud predictions.

---

## 🗺️ Big Picture

```
Real Transaction Data
        │
        ▼
  [Airflow DAG runs]
        │
   ┌────┴────────────────────────────────┐
   │  1. Fetch & Generate Data           │
   │  2. Engineer Features               │
   │  3. Preprocess + SMOTE balance      │
   │  4. Train 3 Models in parallel      │
   │  5. Combine into Ensemble           │
   │  6. Evaluate & save threshold       │
   └────┬────────────────────────────────┘
        │
        ▼
   Trained Model  ──► Spark Inference (real-time scoring)
        │
        ▼
   Dashboard shows FRAUD / NORMAL for every transaction
```

---

## Step 1 — Data Generation (`fetch_data`)

Since this is a simulation, the system **generates synthetic transactions** that mimic real fraud patterns:

| Pattern | Description |
|---------|-------------|
| **Structuring** | Transactions just below $10,000 (avoid reporting thresholds) |
| **Card Testing** | Many tiny transactions to verify a stolen card works |
| **Account Takeover** | Sudden huge transaction after months of normal activity |
| **High-Risk Hour** | Transactions at 2–4 AM (peak fraud window) |
| **High Value** | Unusually large single purchases |

> **100,000 transactions** are generated, with **5% marked as fraud** (5,000 fraud + 95,000 normal).

---

## Step 2 — Feature Engineering

Raw transactions (`amount`, `user_id`, `timestamp`) are transformed into **meaningful signals** that models can learn from:

### Temporal Features
- `hour_sin`, `hour_cos` — time of day encoded as a circle (so 11 PM and 1 AM are "close")
- `day_of_week`, `is_weekend` — fraud spikes on weekends

### Velocity Features (how fast is this user spending?)
- `tx_count_1h` — how many transactions in the last hour
- `tx_count_24h` — how many transactions in the last 24 hours
- `amount_last_1h` — total spent in the last hour

### Statistical / Deviation Features (is this unusual for THIS user?)
- `user_amount_mean` — this user's average transaction amount
- `user_amount_std` — how variable this user's spending usually is
- `amount_z_score` — how many standard deviations above normal this transaction is
- `avg_diff_ratio` — ratio of this amount vs their usual

### Risk Signals
- `terminal_fraud_rate` — historical fraud rate at this merchant terminal
- `amount_x_tx_count` — interaction: high amount + high velocity = danger

### PCA Features (V1–V28)
Statistical noise-reduction components derived from the raw data, similar to what real banks use.

> **Total: 14 numeric features** go into training after dropping non-numeric identifiers.

---

## Step 3 — Preprocessing

```
Raw Features  ──► RobustScaler ──► SMOTE ──► train_data.csv (S3)
```

### RobustScaler
`amount` is normalized using a scaler that ignores outliers (a $50,000 transaction won't distort the scale for $20 transactions).

### SMOTE (Synthetic Minority Oversampling Technique)
With only 5% fraud, models tend to just predict "normal" for everything and still get 95% accuracy. **SMOTE fixes this** by creating synthetic fraud examples:

```
Before SMOTE:  816 samples  →  2 fraud,   814 normal
After SMOTE:  1628 samples  →  814 fraud, 814 normal  (50/50 balanced)
```

SMOTE doesn't copy existing fraud — it **interpolates** between real fraud examples to create new, plausible ones.

---

## Step 4 — Three Models Train in Parallel

Three different algorithms each learn from the same balanced dataset independently:

### 🟠 XGBoost
- Builds **decision trees one at a time**, each one correcting the errors of the previous
- Best at capturing non-linear patterns
- Tuned with `min_child_weight=5` and `gamma=0.1` to prevent memorizing noise

### 🟢 LightGBM
- Similar to XGBoost but grows trees **leaf-first** (faster, handles large datasets better)
- Uses `class_weight='balanced'` as an extra safeguard against imbalance
- L2 regularization (`reg_lambda=0.1`) reduces overfitting

### 🔵 CatBoost
- Handles data with mixed types well
- Built-in handling for categorical features
- Falls back to XGBoost predictions if not installed

Each model outputs a **fraud probability (0.0 → 1.0)** for every test transaction.

---

## Step 5 — Ensemble (Combining the Models)

Instead of trusting just one model, all three predictions are **weighted and averaged**:

```
Ensemble Score = (0.40 × XGBoost) + (0.35 × LightGBM) + (0.25 × CatBoost)
```

XGBoost gets the highest weight because it historically performs best on tabular fraud data.

This approach is more robust — if one model is wrong, the others can outvote it.

---

## Step 6 — Threshold Optimisation

A raw fraud probability (e.g., 0.23) needs to become a binary decision: **FRAUD or NORMAL**.

Instead of using the default 0.5 cutoff, the system **scans every threshold from 0.10 to 0.70** and picks the one that maximises the **F2 score** (which weights catching fraud 2× more than avoiding false alarms):

```
F2 = (1 + 2²) × (Precision × Recall) / (2² × Precision + Recall)
```

This run found: **optimal threshold = 0.15**

> A transaction scoring ≥ 0.15 is flagged as fraud. This threshold is saved to S3 and used by Spark at inference time.

---

## 📊 Final Results (Latest Run)

| Metric | Value | What It Means |
|--------|-------|---------------|
| **Accuracy** | 97.1% | 97 out of 100 transactions classified correctly |
| **Recall** | 72.7% | Catches ~73% of all real fraud cases |
| **Precision** | ~72% | When it says fraud, it's right ~72% of the time |
| **F1** | 84.2% | Balanced score between precision and recall |
| **F2** | 76.9% | Fraud-weighted balance score |
| **ROC-AUC** | 94.5% | Model's ability to rank fraud above normal |
| **PR-AUC** | 90.5% | Precision-recall performance |
| **Cost** | $600 | FP×$5 + FN×$100 (lower = better) |

---

## 🔄 Real-Time Inference (How Spark Uses the Model)

Once the DAG finishes, the Spark streaming job:

1. **Reads** incoming Kafka transactions every 10 seconds
2. **Engineers** the same features (velocity, z-score, hour encoding, etc.)
3. **Applies** the same RobustScaler (loaded from `s3://models/scaler.pkl`)
4. **Loads** the XGBoost model from `s3://models/model.json`
5. **Scores** each transaction — if score ≥ threshold (`s3://models/threshold.json` = 0.15), flags as **FRAUD**
6. Writes results to **PostgreSQL** → visible on the dashboard

---

## 🔁 When Does the Model Retrain?

The Airflow DAG (`fraud_detection_training_enhanced`) is triggered **manually** for now.  
You can also schedule it (e.g., weekly) by setting `schedule_interval='@weekly'` in the DAG definition.

When retrained, the new model automatically replaces the old one in S3 — Spark picks it up on the next batch.
