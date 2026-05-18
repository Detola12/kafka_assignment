"""
processor.py

Faust Streams processor — consumes messages from 'raw-data',
runs the pre-trained fraud detection model on each record,
and publishes predictions to the 'predictions' topic.

Usage:
    faust -A processor worker -l info
"""

import json
import joblib
import numpy as np
import faust
from datetime import datetime, timezone

# ── Load pre-trained model and scaler ─────────────────────────────────────────
print("Loading model and scaler...")
model  = joblib.load("fraud_model.joblib")
scaler = joblib.load("scaler.joblib")
print("Model loaded.\n")

# ── Faust app ─────────────────────────────────────────────────────────────────
app = faust.App(
    "fraud-detector",
    broker="kafka://localhost:9092",
    value_serializer="raw",
)

# ── Topics ────────────────────────────────────────────────────────────────────
raw_topic         = app.topic("raw-data",    value_type=bytes)
predictions_topic = app.topic("predictions", value_type=bytes)

# ── Feature columns (must match training order) ───────────────────────────────
FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount", "Time"]

# ── Streams agent ─────────────────────────────────────────────────────────────
@app.agent(raw_topic)
async def process(stream):
    async for message in stream:
        try:
            data = json.loads(message)

            # Extract features in correct order
            features = [data[f] for f in FEATURES]
            X = np.array(features).reshape(1, -1)

            # Scale Amount (index 28) and Time (index 29)
            X_scaled = X.copy()
            X_scaled[:, 28:30] = scaler.transform(X[:, 28:30])

            # Predict
            prediction  = int(model.predict(X_scaled)[0])
            probability = float(model.predict_proba(X_scaled)[0][1])

            # Build output message
            result = {
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "row":         data.get("timestamp", "unknown"),
                "prediction":  prediction,
                "probability": round(probability, 4),
                "label":       "FRAUD" if prediction == 1 else "LEGITIMATE",
                "amount":      round(data["Amount"], 2),
                "time":        data["Time"],
            }

            # Send to predictions topic
            await predictions_topic.send(value=json.dumps(result).encode())
            print(f"[Processed] {result['label']} | prob: {result['probability']:.4f} | amount: ${result['amount']:.2f}")

        except Exception as e:
            print(f"[Error] Failed to process message: {e}")

if __name__ == "__main__":
    app.main()
