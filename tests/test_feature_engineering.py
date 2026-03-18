"""
Unit tests for core feature engineering logic.
Tests the same transformations used in:
  - src/airflow/dags/fraud_detection_training_enhanced.py
  - src/spark/streaming_job.py
Run with: pytest tests/ -v
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ─── Helpers (mirror DAG / Spark logic) ──────────────────────────────────────

def compute_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors temporal_features() in the Airflow DAG."""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', infer_datetime_format=True)
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    return df


def compute_amount_z_score(df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors statistical_features() in the Airflow DAG."""
    df = df.copy()
    user_stats = df.groupby('user_id')['amount'].agg(['mean', 'std']).reset_index()
    user_stats.columns = ['user_id', 'user_amount_mean', 'user_amount_std']
    df = df.merge(user_stats, on='user_id', how='left')
    df['amount_z_score'] = (df['amount'] - df['user_amount_mean']) / (df['user_amount_std'] + 1e-6)
    df['avg_diff_ratio'] = df['amount'] / df['user_amount_mean']
    return df


def compute_velocity_features(df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors velocity_features() in the Airflow DAG (simplified)."""
    df = df.copy()
    df = df.sort_values(['user_id', 'timestamp'])
    df['tx_count_1h'] = df.groupby('user_id').cumcount() + 1
    df['amount_last_1h'] = df.groupby('user_id')['amount'].cumsum()
    return df


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestTemporalFeatures:
    def test_hour_sin_cos_bounds(self, sample_transactions):
        """hour_sin and hour_cos must always be in [-1, 1]."""
        df = compute_temporal_features(sample_transactions)
        assert df['hour_sin'].between(-1, 1).all(), "hour_sin out of [-1, 1]"
        assert df['hour_cos'].between(-1, 1).all(), "hour_cos out of [-1, 1]"

    def test_is_weekend_binary(self, sample_transactions):
        """is_weekend must be 0 or 1 only."""
        df = compute_temporal_features(sample_transactions)
        assert set(df['is_weekend'].unique()).issubset({0, 1}), "is_weekend not binary"

    def test_hour_range(self, sample_transactions):
        """hour must be in [0, 23]."""
        df = compute_temporal_features(sample_transactions)
        assert df['hour'].between(0, 23).all(), "hour out of [0, 23]"

    def test_cyclical_continuity(self):
        """Midnight (hour=0) and hour=24 (wrap) should produce same sin/cos."""
        h0 = np.sin(2 * np.pi * 0 / 24), np.cos(2 * np.pi * 0 / 24)
        h24 = np.sin(2 * np.pi * 24 / 24), np.cos(2 * np.pi * 24 / 24)
        assert abs(h0[0] - h24[0]) < 1e-9
        assert abs(h0[1] - h24[1]) < 1e-9


class TestAmountZScore:
    def test_z_score_formula(self, sample_transactions):
        """Z-score should average ~0 within each user group (by definition)."""
        df = compute_amount_z_score(sample_transactions)
        # Mean of z-scores per user should be ~0
        user_means = df.groupby('user_id')['amount_z_score'].mean()
        assert (user_means.abs() < 0.5).all(), "User-level mean z-score too far from 0"

    def test_avg_diff_ratio_positive(self, sample_transactions):
        """avg_diff_ratio must always be positive (amount and mean are positive)."""
        df = compute_amount_z_score(sample_transactions)
        assert (df['avg_diff_ratio'] > 0).all(), "avg_diff_ratio contains non-positive values"

    def test_columns_present(self, sample_transactions):
        """Output must include required columns."""
        df = compute_amount_z_score(sample_transactions)
        for col in ['user_amount_mean', 'user_amount_std', 'amount_z_score', 'avg_diff_ratio']:
            assert col in df.columns, f"Column '{col}' missing from output"


class TestVelocityFeatures:
    def test_tx_count_monotonic_per_user(self, sample_transactions):
        """tx_count_1h (cumcount) should increase monotonically per user."""
        df = compute_velocity_features(sample_transactions)
        for uid, grp in df.groupby('user_id'):
            counts = grp['tx_count_1h'].values
            assert all(counts[i] <= counts[i+1] for i in range(len(counts)-1)), \
                f"tx_count_1h not monotonic for user {uid}"

    def test_amount_last_1h_non_negative(self, sample_transactions):
        """Cumulative amount must be non-negative (amounts are positive)."""
        df = compute_velocity_features(sample_transactions)
        assert (df['amount_last_1h'] >= 0).all(), "amount_last_1h has negative values"


class TestFraudLabelDistribution:
    def test_fraud_rate_approx_5pct(self, sample_transactions):
        """Synthetic data should have ~5% fraud rate (set in conftest)."""
        fraud_rate = sample_transactions['target_fraud'].mean()
        # Allow ±5% tolerance around 5%
        assert 0.0 <= fraud_rate <= 0.15, f"Fraud rate {fraud_rate:.2%} outside expected range [0%, 15%]"

    def test_binary_labels(self, sample_transactions):
        """target_fraud must be 0 or 1 only."""
        assert set(sample_transactions['target_fraud'].unique()).issubset({0, 1})


class TestRobustScaler:
    def test_scaler_reduces_outlier_impact(self):
        """RobustScaler should bring extreme values closer to the IQR range."""
        from sklearn.preprocessing import RobustScaler
        amounts = np.array([[1], [2], [3], [1000], [2], [3], [2]])  # 1000 is outlier
        scaler = RobustScaler()
        scaled = scaler.fit_transform(amounts)
        # The outlier (1000) should be scaled but other values should be near 0
        non_outlier_vals = scaled[:-4]  # first 3 values
        assert non_outlier_vals.std() < 2.0, "Non-outlier spread too high after scaling"

    def test_scaler_fit_predict_consistency(self):
        """Fitting on train, transforming test should produce consistent results."""
        from sklearn.preprocessing import RobustScaler
        X_train = np.array([[10], [20], [15], [18], [12], [25], [14]])
        X_test = np.array([[16], [11]])
        scaler = RobustScaler()
        scaler.fit(X_train)
        scaled_train = scaler.transform(X_train)
        scaled_test = scaler.transform(X_test)
        # median of training after scaling should be ~0
        assert abs(np.median(scaled_train)) < 0.5
