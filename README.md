# AI-Powered IoT Fleet Logistics & Predictive Maintenance Pipeline

A production-grade, end-to-end data engineering and AI system that processes real-time telemetry streams from a commercial vehicle fleet to predict components at risk of failure.

## 🏗️ System Architecture Overview
* **Ingestion:** Asynchronous Python edge simulator streaming to a distributed message broker.
* **Storage & Processing:** Delta Lake (Bronze/Silver/Gold architecture) managed via Apache Spark.
* **AI & MLOps:** Predictive modeling via XGBoost/LightGBM, governed with MLflow, and served via a FastAPI microservice.

## 🏃‍♂️ Phase 1: Local Ingestion Setup
To execute the live concurrent simulation:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m ingestion.async_simulator
