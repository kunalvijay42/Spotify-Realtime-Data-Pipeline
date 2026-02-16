# Spotify-Realtime-Data-Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.4-black.svg)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE.svg)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DWH-29B5E8.svg)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-1.5+-FF694B.svg)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

## 📌 Project Overview

This project implements a **production-grade, end-to-end real-time Spotify Data Pipeline** using a **modern data stack**. It simulates high-volume streaming music events and processes them through scalable ingestion, transformation, and analytics layers.

The pipeline is fully automated and designed to mirror **real-world data platforms**.

---

## 🏗️ Technical Architecture

<img width="5600" height="2898" alt="Architectur" src="https://github.com/kunalvijay42/Spotify-Realtime-Data-Pipeline/blob/main/Spotify%20Real%20Time%20Pipeline%20Architecture%20Diagram.png" />

---

## 🚀 Data Pipeline Flow

The system implements a **medallion architecture** with the following components:

1. **Data Generation Layer**  
   Synthetic Spotify streaming events (user activity, track metadata, geographic location, device information) generated via Python Faker library

2. **Streaming Layer**  
   Real-time event streaming through Apache Kafka topics with configurable throughput and partitioning

3. **Ingestion Layer**  
   Kafka consumers persist raw events to MinIO object storage (S3-compatible) for reliable data lake storage

4. **Orchestration Layer**  
   Apache Airflow DAGs automate incremental data loading from MinIO to Snowflake Bronze layer on scheduled intervals

5. **Data Warehouse Layer**  
   Snowflake manages data across three layers following medallion architecture:
   - **Bronze:** Raw, unprocessed streaming data
   - **Silver:** Cleaned, validated, and conformed data
   - **Gold:** Business-ready aggregated analytics tables

6. **Transformation Layer**  
   dbt (data build tool) performs SQL-based transformations and data quality testing directly within Snowflake

---

## 👩‍💻 Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Generation** | Python (Faker) | Synthetic streaming event generation |
| **Message Streaming** | Apache Kafka | Real-time event streaming platform |
| **Object Storage** | MinIO | S3-compatible data lake storage |
| **Data Warehouse** | Snowflake | Cloud-native data warehouse |
| **Transformation** | dbt (data build tool) | SQL transformations, testing, documentation |
| **Orchestration** | Apache Airflow | Workflow automation and scheduling |
| **Containerization** | Docker & Docker Compose | Reproducible infrastructure deployment |

---

## 📂 Github Repository Structure

```text
spotify-realtime-data-pipeline/
├── docker/ # Airflow DAGs for orchestration
│   ├── .env
│   ├── docker-compose.yml
│   └── dags/
│       ├── minio-to-kafka.py
│       └── .env
├── dbt/  # DBT Files
│   └── models/
│       ├── gold/
│       ├── silver/
│       └── sources.yml
├── simulator/ # Python Simulator Producer
│   ├── producer.py
│   └── .env
├── consumer/ # Kafka Consumer 
│   ├── kafka-to-minio.py
│   └── .env
├── requirements.txt
└── README.md
```
---
## 🚀 Steps to run the Project

```bash
# Clone the repository
git clone https://github.com/kunalvijay42/Spotify-Realtime-Data-Pipeline.git

# Navigate to project directory
cd Spotify-Realtime-Data-Pipeline

# Start all services
docker-compose up -d
```
---