"""
score_and_sync.py
End-to-end Salesforce lead scoring and routing pipeline.

Flow:
  1. Train a logistic regression model on synthetic labeled data
  2. Seed 100 mock leads into a Salesforce Developer Org
  3. Pull those leads back via SOQL
  4. Score each lead (0–100) using the trained model
  5. Bulk-write Lead_Score__c back to Salesforce
  6. The native Record-Triggered Flow in SFDC then:
       - Sets Lead_Score_Tier__c (Hot / Warm / Cold)
       - Routes Hot leads to "Hot Leads Queue"
       - Creates follow-up Tasks per tier

Prerequisites:
  - pip install simple_salesforce scikit-learn pandas faker python-dotenv
  - A Salesforce Developer Org with:
      - Lead_Score__c    (Number field, 0–100)
      - Lead_Score_Tier__c (Picklist: Hot / Warm / Cold)
      - A Queue named "Hot Leads Queue"
  - .env file with: SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_DOMAIN
"""

import os
import sys
import json
import random
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from faker import Faker
from simple_salesforce import Salesforce, SalesforceLogin
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

load_dotenv()
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# ── Constants ───────────────────────────────────────────────────────────────

LEAD_SOURCES = ["Web", "Referral", "Cold Outbound", "Event", "Paid Ad", "Partner"]
INDUSTRIES   = [
    "Technology", "Financial Services", "Healthcare", "Manufacturing",
    "Retail", "Professional Services", "Education", "Non-Profit",
]
SOURCE_QUALITY = {
    "Referral": 0.72, "Web": 0.55, "Event": 0.50,
    "Partner": 0.45, "Paid Ad": 0.30, "Cold Outbound": 0.22,
}
INDUSTRY_WEIGHT = {
    "Technology": 0.70, "Financial Services": 0.65,
    "Professional Services": 0.60, "Healthcare": 0.50,
    "Manufacturing": 0.45, "Retail": 0.35,
    "Education": 0.25, "Non-Profit": 0.15,
}

MODEL_PATH = Path("data/model.joblib")
TRAINING_DATA_PATH = Path("data/training_data.csv")


# ── Step 1: Train model ──────────────────────────────────────────────────────

def encode_features(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw lead fields to numeric feature matrix."""
    df = df.copy()
    df["source_quality"]   = df["LeadSource"].map(SOURCE_QUALITY).fillna(0.3)
    df["industry_weight"]  = df["Industry"].map(INDUSTRY_WEIGHT).fillna(0.3)
    df["log_employees"]    = np.log1p(df["NumberOfEmployees"].fillna(1))
    df["log_revenue"]      = np.log1p(df["AnnualRevenue"].fillna(0))
    return df[["source_quality", "industry_weight", "log_employees", "log_revenue"]]


def train_model() -> Pipeline:
    """Train logistic regression on synthetic data; return fitted pipeline."""
    if not TRAINING_DATA_PATH.exists():
        print("Training data not found. Run generate_training_data.py first.")
        sys.exit(1)

    df = pd.read_csv(TRAINING_DATA_PATH)
    X  = encode_features(df)
    y  = df["converted"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf",    LogisticRegression(max_iter=500, random_state=42)),
    ])
    model.fit(X_train, y_train)

    print("\n── Model evaluation (held-out 20%) ──")
    print(classification_report(y_test, model.predict(X_test)))

    MODEL_PATH.parent.mkdir(exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}\n")
    return model


def load_or_train_model() -> Pipeline:
    if MODEL_PATH.exists():
        print(f"Loading cached model from {MODEL_PATH}")
        return joblib.load(MODEL_PATH)
    return train_model()


def predict_score(model: Pipeline, df: pd.DataFrame) -> np.ndarray:
    """Return conversion probability scaled to 0–100."""
    X = encode_features(df)
    proba = model.predict_proba(X)[:, 1]
    return np.round(proba * 100).astype(int)


# ── Step 2: Salesforce auth ──────────────────────────────────────────────────

def connect_to_salesforce() -> Salesforce:
    session_id   = os.environ["SF_SESSION_ID"]
    instance_url = os.environ["SF_INSTANCE_URL"]

    print(f"Connecting to Salesforce via session ID…")
    sf = Salesforce(
        session_id=session_id,
        instance_url=instance_url,
    )
    print(f"Connected. Instance: {sf.sf_instance}\n")
    return sf


# ── Step 3: Seed 100 mock leads ──────────────────────────────────────────────

def build_mock_lead(i: int) -> dict:
    source   = random.choice(LEAD_SOURCES)
    industry = random.choice(INDUSTRIES)
    employees = max(1, int(np.random.lognormal(mean=4.5, sigma=1.2)))
    revenue   = max(50_000, int(np.random.lognormal(mean=13.5, sigma=1.5)))

    return {
        "LastName":           fake.last_name(),
        "FirstName":          fake.first_name(),
        "Company":            fake.company(),
        "Title":              random.choice(["VP Sales", "Director of Marketing", "CEO", "Head of Growth", "CRO", "Founder"]),
        "Email":              fake.company_email(),
        "Phone":              fake.phone_number(),
        "Industry":           industry,
        "LeadSource":         source,
        "NumberOfEmployees":  employees,
        "AnnualRevenue":      float(revenue),
        "Description":        f"Mock lead #{i:03d} — seeded by salesforce-lead-routing-pipeline",
    }


def seed_leads(sf: Salesforce, n: int = 100) -> list[str]:
    """Create n mock leads in SFDC; return list of created Lead IDs."""
    print(f"Seeding {n} mock leads into Salesforce…")
    lead_ids = []
    errors   = []

    for i in range(1, n + 1):
        payload = build_mock_lead(i)
        try:
            result = sf.Lead.create(payload)
            if result.get("success"):
                lead_ids.append(result["id"])
            else:
                errors.append(result)
        except Exception as e:
            errors.append(str(e))

    print(f"  Created {len(lead_ids)}/{n} leads successfully.")
    if errors:
        print(f"  {len(errors)} errors: {errors[:3]}")
    return lead_ids


# ── Step 4: Pull leads back via SOQL ────────────────────────────────────────

def fetch_leads(sf: Salesforce, lead_ids: list[str]) -> pd.DataFrame:
    """Query SFDC for the seeded leads and return as DataFrame."""
    id_list  = ", ".join(f"'{lid}'" for lid in lead_ids)
    soql     = (
        f"SELECT Id, LastName, Company, Industry, LeadSource, "
        f"NumberOfEmployees, AnnualRevenue "
        f"FROM Lead "
        f"WHERE Id IN ({id_list})"
    )
    print(f"\nFetching {len(lead_ids)} leads via SOQL…")
    result = sf.query_all(soql)
    records = result["records"]
    df = pd.DataFrame(records).drop(columns=["attributes"], errors="ignore")
    df["NumberOfEmployees"] = pd.to_numeric(df["NumberOfEmployees"], errors="coerce").fillna(1)
    df["AnnualRevenue"]     = pd.to_numeric(df["AnnualRevenue"],     errors="coerce").fillna(0)
    print(f"Fetched {len(df)} records.\n")
    return df


# ── Step 5: Bulk-write scores back to SFDC ──────────────────────────────────

def write_scores_to_sfdc(sf: Salesforce, df: pd.DataFrame, scores: np.ndarray) -> None:
    """Bulk-update Lead_Score__c for each lead."""
    updates = [
        {"Id": row["Id"], "Lead_Score__c": int(score)}
        for (_, row), score in zip(df.iterrows(), scores)
    ]

    print(f"Writing {len(updates)} scores back to Salesforce (bulk API)…")
    results = sf.bulk.Lead.update(updates, batch_size=200, use_serial=False)

    success = sum(1 for r in results if r.get("success"))
    print(f"  Updated {success}/{len(updates)} records successfully.")

    if success < len(updates):
        failures = [r for r in results if not r.get("success")]
        print(f"  Failures (first 3): {failures[:3]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Salesforce Lead Scoring & Routing Pipeline")
    print("=" * 60)

    # 1. Train / load model
    model = load_or_train_model()

    # 2. Connect to Salesforce
    sf = connect_to_salesforce()

    # 3. Seed 100 mock leads
    lead_ids = seed_leads(sf, n=100)
    if not lead_ids:
        print("No leads created. Exiting.")
        sys.exit(1)

    # 4. Pull leads back
    df = fetch_leads(sf, lead_ids)

    # 5. Score
    scores = predict_score(model, df)
    hot  = (scores >= 75).sum()
    warm = ((scores >= 50) & (scores < 75)).sum()
    cold = (scores < 50).sum()
    print(f"Score distribution — Hot: {hot}  Warm: {warm}  Cold: {cold}")

    # 6. Write scores back to SFDC
    write_scores_to_sfdc(sf, df, scores)

    print("\nPipeline complete.")
    print("The native SFDC Record-Triggered Flow will now:")
    print("  • Set Lead_Score_Tier__c (Hot / Warm / Cold)")
    print("  • Assign Hot leads to 'Hot Leads Queue'")
    print("  • Create follow-up Tasks for Hot and Warm leads")
    print("=" * 60)

    # Save local summary for reference
    df["Lead_Score__c"] = scores
    df["Tier"] = pd.cut(
        scores,
        bins=[-1, 49, 74, 100],
        labels=["Cold", "Warm", "Hot"],
    )
    out = Path("data/scored_leads.csv")
    df.to_csv(out, index=False)
    print(f"Local summary saved to {out}")


if __name__ == "__main__":
    main()
