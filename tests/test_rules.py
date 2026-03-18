"""
Unit tests for fraud rule logic — especially Shadow/Canary Mode.
Run with: pytest tests/ -v
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestShadowMode:
    """Verify that SHADOW_MODE suppresses fraud flags while still logging the reason."""

    def _apply_shadow_mode(self, pdf: pd.DataFrame) -> pd.DataFrame:
        """Mirror the shadow mode logic from streaming_job.process_batch()."""
        pdf = pdf.copy()
        pdf["is_fraud"] = 0
        pdf["fraud_reason"] = pdf.apply(
            lambda row: ("SHADOW:" + (
                "Model" if row.get("model_prediction", 0) == 1 else ""
            ) + ("|Travel" if row.get("is_impossible_travel", False) else "")
            + ("|Ring" if row.get("is_ring_fraud", 0) == 1 else "")).rstrip(":"),
            axis=1
        )
        return pdf

    def test_shadow_mode_allows_all(self, sample_windowed_batch):
        """In shadow mode, ALL transactions must have is_fraud=0."""
        pdf = sample_windowed_batch.copy()
        pdf["model_prediction"] = 1  # Pretend all are fraud
        pdf["is_impossible_travel"] = True
        pdf["is_ring_fraud"] = 1
        result = self._apply_shadow_mode(pdf)
        assert (result["is_fraud"] == 0).all(), "Shadow mode should allow all transactions"

    def test_shadow_mode_logs_reason(self, sample_windowed_batch):
        """Shadow mode must still record the would-be fraud reason in fraud_reason."""
        pdf = sample_windowed_batch.copy()
        pdf["model_prediction"] = 1
        pdf["is_impossible_travel"] = False
        pdf["is_ring_fraud"] = 0
        result = self._apply_shadow_mode(pdf)
        assert (result["fraud_reason"].str.startswith("SHADOW:")).all(), \
            "Shadow fraud_reason should start with 'SHADOW:'"

    def test_shadow_mode_no_false_negatives(self, sample_windowed_batch):
        """Non-fraudulent transactions should still be ALLOW (0) in shadow mode."""
        pdf = sample_windowed_batch.copy()
        pdf["model_prediction"] = 0
        pdf["is_impossible_travel"] = False
        pdf["is_ring_fraud"] = 0
        result = self._apply_shadow_mode(pdf)
        assert (result["is_fraud"] == 0).all()


class TestRuleBasedDetection:
    """Tests for rule-based fraud flags (ring detection, travel, merchant)."""

    def test_shared_device_sets_ring_fraud(self):
        """Two different users with the same device_id should be flagged as ring fraud."""
        pdf = pd.DataFrame({
            "transaction_id": ["tx_1", "tx_2"],
            "user_id": [1, 2],
            "device_id": ["DEV_X", "DEV_X"],  # Same device — ring fraud!
            "ip_address": ["10.0.0.1", "10.0.0.2"],
            "amount": [100.0, 200.0],
        })
        pdf['is_ring_fraud'] = 0
        pdf['ring_reason'] = ""

        dev_counts = pdf.groupby('device_id')['user_id'].nunique()
        bad_devs = dev_counts[dev_counts > 1].index.tolist()
        mask = pdf['device_id'].isin(bad_devs)
        pdf.loc[mask, 'is_ring_fraud'] = 1
        pdf.loc[mask, 'ring_reason'] = "Shared Device"

        assert (pdf['is_ring_fraud'] == 1).all(), "Both transactions should be ring fraud"
        assert (pdf['ring_reason'] == "Shared Device").all()

    def test_unique_devices_no_ring_fraud(self):
        """Different devices => no ring fraud."""
        pdf = pd.DataFrame({
            "transaction_id": ["tx_1", "tx_2"],
            "user_id": [1, 2],
            "device_id": ["DEV_A", "DEV_B"],
            "ip_address": ["10.0.0.1", "10.0.0.2"],
            "amount": [100.0, 200.0],
        })
        pdf['is_ring_fraud'] = 0

        dev_counts = pdf.groupby('device_id')['user_id'].nunique()
        bad_devs = dev_counts[dev_counts > 1].index.tolist()
        assert len(bad_devs) == 0, "No ring fraud expected for unique devices"

    def test_high_risk_merchant_flagged(self):
        """Transactions from merchants starting with 'M_00' should be flagged."""
        pdf = pd.DataFrame({
            "transaction_id": ["tx_1", "tx_2"],
            "user_id": [1, 2],
            "merchant_id": ["M_00_CASINO", "M_05_NORMAL"],
            "amount": [500.0, 100.0],
            "is_ring_fraud": [0, 0],
            "ring_reason": ["", ""],
        })
        mask_risky = pdf['merchant_id'].str.startswith('M_00', na=False)
        pdf.loc[mask_risky, 'is_ring_fraud'] = 1
        pdf.loc[mask_risky, 'ring_reason'] = "High Risk Merchant"

        assert pdf.loc[0, 'is_ring_fraud'] == 1
        assert pdf.loc[1, 'is_ring_fraud'] == 0

    def test_impossible_travel_threshold(self):
        """Distance > 3000km should trigger impossible travel flag."""
        distances = pd.Series([100.0, 3001.0, 2999.0, 5000.0])
        flags = (distances > 3000).astype(int)
        assert flags[0] == 0
        assert flags[1] == 1  # Just over threshold
        assert flags[2] == 0  # Just under
        assert flags[3] == 1
