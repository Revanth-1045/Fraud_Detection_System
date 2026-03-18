import pandas as pd
import numpy as np
import random

# Settings
NUM_SAMPLES = 1000
OUTPUT_FILE = "data/mock_creditcard.csv"

# Generate Mock Data
# Schema: Time, V1-V28, Amount, Class
data = {
    "Time": np.arange(NUM_SAMPLES),
    "Amount": np.random.uniform(0, 500, NUM_SAMPLES)
}

# Generate V1-V28 (PCA components - normally distributed)
for i in range(1, 29):
    data[f"V{i}"] = np.random.normal(0, 1, NUM_SAMPLES)

# Generate Class (Target) - 0.5% Fraud
data["Class"] = [1 if random.random() < 0.005 else 0 for _ in range(NUM_SAMPLES)]

# Create DataFrame
df = pd.DataFrame(data)

# Save
import os
os.makedirs("data", exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False)
print(f"Generated {NUM_SAMPLES} samples to {OUTPUT_FILE}")
