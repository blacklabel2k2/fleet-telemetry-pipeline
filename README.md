# Real-Time Event-Driven IoT Fleet Telemetry & Predictive Maintenance Pipeline

## 📌 Project Overview
This project is a production-grade, real-time streaming data pipeline designed to ingest, process, and analyze continuous IoT telemetry data from a large logistics vehicle fleet. Built using a **Medallion Lakehouse Architecture (Bronze ➔ Silver)**, the system processes high-velocity sensor data (engine temperatures, vibration amplitudes, fuel flow rates, and GPS coordinates) to establish an AI-ready data foundation for predictive maintenance models.

---

## 🏗️ System Architecture & Data Flow



The data journey through the pipeline is decoupled into three structural milestones:

1. **Intelligent Edge Ingestion:** An asynchronous simulator models 50+ vehicles concurrently broadcasting data. A strict data firewall enforces schemas at the edge before hitting the network.
2. **Distributed Message Buffering:** A Dockerized message broker acts as a high-speed shock absorber, distributing incoming messages evenly across parallel lanes using deterministic key routing.
3. **Structured Lakehouse Processing:** A local distributed computing engine runs continuous multi-stream queries to process data in micro-batches, archiving raw streams and refining them into typed, deduplicated historical records.

---

## 🛠️ Tech Stack & Infrastructure
* **Language:** Python 3.10+
* **Concurrency:** Asyncio (for high-throughput simulation)
* **Data Validation:** Pydantic v2 (Strict runtime schema enforcement)
* **Containerization:** Docker & Docker Compose
* **Message Broker:** Apache Kafka (Distributed event streaming)
* **Compute Engine:** Apache Spark 3.5.0 (PySpark Structured Streaming)
* **Storage Layer:** Delta Lake 3.2.0 (ACID compliant storage over Parquet)

---

## 💾 Lakehouse Data Layer Definitions

### 1. Bronze Layer (`storage/bronze_fleet_telemetry`)
* **Type:** Append-Only Archive
* **Format:** Delta Lake
* **Description:** Acts as the immutable "Source of Truth." It preserves the original, unaltered raw binary payloads directly from the message broker alongside infrastructure metadata timestamps. This guarantees that historical state can be replayed perfectly in the event of an upstream disaster or schema change.

### 2. Silver Layer (`storage/silver_fleet_telemetry`)
* **Type:** Cleaned & Enforced Analytical Table
* **Format:** Delta Lake
* **Description:** The refined data zone. This layer applies three major data refinery concepts in real time:
  * **JSON Schema Parsing:** Unpacks binary JSON bytes and casts fields into precise data types (`DoubleType`, `TimestampType`).
  * **10-Minute Watermarking:** Manages late-arriving packets by keeping a rolling state window open for delayed network transmissions while protecting local memory.
  * **Streaming Deduplication:** Drops duplicate events caused by network retries based on a unique composite key (`vehicle_id` + `event_timestamp`).

### 3. Gold Layer (`storage/gold_fault_features`)
* **Type:** Real-Time Feature Aggregation Table
* **Format:** Delta Lake
* **Description:** The business-intelligence and AI-primed zone. This layer reads continuously from the Silver stream and computes rolling 5-minute Tumbling Windows backed by a 10-minute watermark. It exposes real-time statistical metrics—such as engine temperature moving averages and vibration standard deviations (`vibration_anomaly_score`)—acting as a live feature store for downstream predictive maintenance Machine Learning models.
---

## 🚀 Local Deployment Guide

### Prerequisites
Ensure you have the following installed on your machine:
* Docker & Docker Compose
* Python 3.10+
* Java OpenJDK 11 or 17 (Required for running the local Apache Spark engine)

### 1. Environment Setup
Clone the repository and spin up a Python virtual environment:
```bash
git clone <your-repository-url>
cd fleet-telemetry-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
### 2. Launch Infrastructure (Kafka)
Spin up the decoupled messaging layer containers in the background:

```bash
docker compose up -d
```

### 3. Run the Streaming Pipeline
Open two separate terminal windows with your virtual environment active.
* Terminal 1 (Data Generation): Launch the edge simulator to start streaming telemetry:
``` bash
python -m ingestion.async_simulator
```
* Terminal 2 (Distributed Processor): Launch the Spark engine to continuously update your Bronze and Silver Delta tables:
``` bash
python -m processing.spark_stream_processor
```

### Verifying Storage
Your storage directory will automatically generate the following schema blueprint:
``` plaintext
storage/
├── checkpoints/
│   ├── bronze/
│   └── silver/
├── bronze_fleet_telemetry/
│   └── _delta_log/
└── silver_fleet_telemetry/
    └── _delta_log/

```
To run static smoke tests on your storage layers, execute python test_read_silver.py.

## 🤖 Predictive Maintenance Machine Learning Layer

With the Gold Layer acting as a live feature store, a dual-modeling script (`train_predictive_models.py`) was implemented using `scikit-learn` to execute both classification and regression training simultaneously:

1. **Failure State Classifier (Random Forest)**
   * **Objective:** Predict whether an asset is healthy (0) or failing (1) based on moving averages.
   * **Performance:** **100% Accuracy** across testing subsets due to clear feature boundaries.

2. **Remaining Useful Life (RUL) Regressor (Random Forest)**
   * **Objective:** Forecast the exact number of hours left before an asset component requires service.
   * **Performance:** **Mean Absolute Error (MAE) of 0.27 hours** (~16 minutes), enabling precise operational maintenance scheduling.