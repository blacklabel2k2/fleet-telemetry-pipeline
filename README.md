# AI-Powered IoT Fleet Logistics & Predictive Maintenance Pipeline

A production-grade, end-to-end data engineering and AI system that processes real-time telemetry streams from a commercial vehicle fleet to predict components at risk of failure.

## 🏗️ System Architecture Overview
The pipeline maps directly to standard modern real-time data platform architectures:



[async_simulator.py] ──▶ [producer.py] ──▶ [Kafka Broker (Topic: fleet_telemetry)]
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3


## 🛠️ Tech Stack & Core Infrastructure
* **Ingestion:** Asynchronous Python (utilizing `asyncio` for concurrent device simulation).
* **Data Quality Layer:** `Pydantic` for strict type and schema validation.
* **Message Broker:** `Apache Kafka` deployed via containerized `Docker Compose` architecture, utilizing 4 distinct partitions for balanced processing.

## 🏃‍♂️ How to Run the Infrastructure Local Environment

### 1. Boot up the Streaming Infrastructure
Ensure Docker Desktop is active on your machine, then spin up the containerized message broker:
```bash
docker compose up -d

To verify the health and listeners of the broker, monitor the internal logs:

Bash
docker logs fleet-kafka

### 2. Initialize the Stream Generator
Activate your virtual environment and execute the asynchronous stream engine:

Bash
source venv/bin/activate
python -m ingestion.async_simulator

## 📈 Milestone Achievements: Phase 1 & 2 Complete

[x] Engineered concurrent data production mimicking 50+ individual edge devices.

[x] Established strict validation guards preventing malformed JSON ingestion.

[x] Configured deterministic partition routing using the vehicle identifier as the message key.

[x] Implemented a self-healing broker recovery process using Docker volumes.