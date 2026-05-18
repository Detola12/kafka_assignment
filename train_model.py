"""
train_model.py

Run this ONCE before starting the pipeline.
It trains a Random Forest on the credit card fraud dataset,
evaluates it, and saves the model + scaler to disk.

Usage:
    python train_model.py
"""

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
import joblib

# ── Load data ────────────────────────────────────────────────────────────────
print("Loading dataset...")
df = pd.read_csv("creditcard.csv")
print(f"Dataset shape: {df.shape}")
print(f"Fraud cases: {df['Class'].sum()} ({df['Class'].mean()*100:.3f}%)\n")

# ── Features and target ───────────────────────────────────────────────────────
X = df.drop(columns=["Class"])
y = df["Class"]

# Scale Amount and Time (V1-V28 are already PCA-scaled)
scaler = StandardScaler()
X[["Amount", "Time"]] = scaler.fit_transform(X[["Amount", "Time"]])

# ── Train/test split ──────────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ── Train model ───────────────────────────────────────────────────────────────
print("Training Random Forest (this may take a minute)...")
model = RandomForestClassifier(
    n_estimators=100,
    class_weight="balanced",  # handles class imbalance
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

# ── Evaluate ──────────────────────────────────────────────────────────────────
print("\nModel Performance:")
print("=" * 50)
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, target_names=["Legitimate", "Fraud"]))

# ── Save model and scaler ─────────────────────────────────────────────────────
joblib.dump(model, "fraud_model.joblib")
joblib.dump(scaler, "scaler.joblib")
print("Saved: fraud_model.joblib")
print("Saved: scaler.joblib")
