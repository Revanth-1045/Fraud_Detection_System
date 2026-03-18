"""
Shared pytest fixtures for Phase 10 unit tests.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


@pytest.fixture
def sample_transactions():
    """Returns a small DataFrame mimicking the producer output schema."""
    np.random.seed(42)
    n = 200
    start = datetime(2024, 1, 1, 8, 0, 0)

    records = []
    for i in range(n):
        records.append({
            "transaction_id": f"tx_{i}",
            "user_id": np.random.randint(1, 20),
            "amount": np.random.uniform(1, 1000),
            "terminal_id": np.random.randint(1, 10),
            "timestamp": (start + timedelta(seconds=i * 30)).isoformat(),
            "target_fraud": 1 if np.random.random() < 0.05 else 0,
        })
        # Add V1-V28
        for j in range(1, 29):
            records[-1][f"V{j}"] = np.random.normal(0, 1)

    return pd.DataFrame(records)


@pytest.fixture
def sample_windowed_batch():
    """Returns a small pandas DataFrame simulating a Spark foreachBatch PDF."""
    np.random.seed(7)
    n = 20
    now = datetime.now()
    data = {
        "transaction_id": [f"win_tx_{i}" for i in range(n)],
        "user_id": np.random.randint(1, 10, n),
        "amount": np.random.uniform(10, 500, n),
        "timestamp": [(now - timedelta(minutes=i)).isoformat() for i in range(n)],
        "calc_tx_count_1h": np.random.randint(1, 15, n),
        "calc_amount_last_1h": np.random.uniform(100, 5000, n),
        "lat": np.random.uniform(25, 48, n),
        "lon": np.random.uniform(-125, -65, n),
        "ip_address": [f"10.0.0.{i % 5}" for i in range(n)],
        "device_id": [f"DEV_{i % 4}" for i in range(n)],
        "merchant_id": [f"M_{i % 10:02d}" for i in range(n)],
        "is_ring_fraud": [0] * n,
        "ring_reason": [""] * n,
        "distance_from_center": np.random.uniform(100, 2000, n),
        "model_prediction": np.random.randint(0, 2, n),
    }
    for j in range(1, 29):
        data[f"V{j}"] = np.random.normal(0, 1, n)
    return pd.DataFrame(data)
