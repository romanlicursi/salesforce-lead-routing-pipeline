"""
generate_training_data.py
Generates synthetic labeled training data for the lead scoring logistic regression model.
Run once before score_and_sync.py to produce training_data.csv.
"""

import pandas as pd
import numpy as np
import random

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

LEAD_SOURCES = ["Web", "Referral", "Cold Outbound", "Event", "Paid Ad", "Partner"]

# Source quality weights — higher = more likely to be a hot lead
SOURCE_QUALITY = {
    "Referral":      0.72,
    "Web":           0.55,
    "Event":         0.50,
    "Partner":       0.45,
    "Paid Ad":       0.30,
    "Cold Outbound": 0.22,
}

INDUSTRIES = [
    "Technology", "Financial Services", "Healthcare", "Manufacturing",
    "Retail", "Professional Services", "Education", "Non-Profit",
]

INDUSTRY_WEIGHT = {
    "Technology":            0.70,
    "Financial Services":    0.65,
    "Professional Services": 0.60,
    "Healthcare":            0.50,
    "Manufacturing":         0.45,
    "Retail":                0.35,
    "Education":             0.25,
    "Non-Profit":            0.15,
}


def generate_record(i: int) -> dict:
    source = random.choice(LEAD_SOURCES)
    industry = random.choice(INDUSTRIES)
    employees = int(np.random.lognormal(mean=4.5, sigma=1.2))
    employees = max(1, min(employees, 50_000))
    revenue = int(np.random.lognormal(mean=13.5, sigma=1.5))
    revenue = max(50_000, min(revenue, 500_000_000))

    # Probability of conversion based on signals
    p = (
        0.30
        + 0.25 * SOURCE_QUALITY[source]
        + 0.20 * INDUSTRY_WEIGHT[industry]
        + 0.15 * min(employees / 500, 1.0)
        + 0.10 * min(revenue / 10_000_000, 1.0)
        + np.random.normal(0, 0.08)
    )
    p = float(np.clip(p, 0.0, 1.0))
    label = int(np.random.random() < p)

    return {
        "lead_id":          f"TRAIN-{i:04d}",
        "LeadSource":       source,
        "Industry":         industry,
        "NumberOfEmployees": employees,
        "AnnualRevenue":    revenue,
        "converted":        label,
    }


def main():
    records = [generate_record(i) for i in range(1, 1001)]
    df = pd.DataFrame(records)

    out_path = "data/training_data.csv"
    import os
    os.makedirs("data", exist_ok=True)
    df.to_csv(out_path, index=False)

    total = len(df)
    converted = df["converted"].sum()
    print(f"Generated {total} training records — {converted} converted ({converted/total:.1%})")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
