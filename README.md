# Credit Card Fraud Detection — Real-Time Kafka Pipeline

ENGR 5785G | Assignment 1

Dataset: Credit Card Fraud (Kaggle)  
Streams library: Faust (Python)  
Model: Random Forest

---

## Files

- `producer.py` — reads creditcard.csv and sends each row to Kafka at 1 row/sec
- `processor.py` — Faust agent that runs the fraud model on each incoming message
- `consumer.py` — prints predictions to the console as they arrive
- `train_model.py` — trains the model offline and saves it to disk
- `fraud_model.joblib` — saved model
- `scaler.joblib` — saved scaler for Amount and Time columns
- `docker-compose.yml` — runs Kafka locally

---

## Steps

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Download the dataset**

Get `creditcard.csv` from [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) and put it in the project folder.

**3. Train the model**
```bash
python train_model.py
```

**4. Start Kafka**
```bash
docker compose up -d
```

**5. Open 3 terminals and run in this order**

Terminal 1:
```bash
faust -A processor worker -l info
```

Terminal 2:
```bash
python producer.py
```

Terminal 3:
```bash
python consumer.py
```

---

## Model performance

| | Legitimate | Fraud |
|---|------------|-------|
| Precision | 1.00       | 0.96  |
| Recall | 1.00       | 0.74  |
| F1 | 1.00       | 0.84  |

Accuracy: 1.00

---

## Demo

https://ontariotechu-my.sharepoint.com/:v:/g/personal/emmanuel_adegboye_ontariotechu_net/IQBaijYf8aKPQq9jXmX0Pd95Ad2-A50QUK1DrQ1kK6Pwayo?nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJPbmVEcml2ZUZvckJ1c2luZXNzIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXciLCJyZWZlcnJhbFZpZXciOiJNeUZpbGVzTGlua0NvcHkifX0&e=b9hZGX
