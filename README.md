# 🛒 E-Commerce Analytics Platform (End-to-End Data Engineering Project)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red)
![Databricks](https://img.shields.io/badge/Databricks-ETL-orange)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20SNS-yellow)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-green)

---

## 📌 Overview

This project demonstrates a **production-grade data engineering pipeline** for an e-commerce platform.

It simulates real-world data ingestion, transformation, and analytics using:

- 🧱 **Lakehouse Architecture (Bronze → Silver → Gold)**
- 🔄 **Batch processing with incremental loads**
- ⚙️ **Workflow orchestration using Apache Airflow (Astro + Docker)**
- ☁️ **Cloud storage using AWS S3**
- ⚡ **Distributed processing using Databricks (PySpark)**

---

# 🏗️ Architecture (with Icons)

    🧾 Source Data (CSV Files)
                │
                ▼
    ☁️ AWS S3 (Staging Layer)
                │
                ▼
    🔄 Airflow (Astro + Docker on EC2)
    ├── 📦 Move Files (S3 → Raw)
    ├── ⏳ S3 Sensor (wait for files)
    ├── 🚀 Trigger Databricks Jobs
    └── 🔔 SNS Notifications
                │
                ▼
    ⚡ Databricks (PySpark Processing)
    ├── 🟤 Bronze Layer (Raw Ingestion)
    ├── ⚪ Silver Layer (Cleaning & Joins)
    └── 🟡 Gold Layer (Aggregations & KPIs)
                │
                ▼
    🧊 Delta Lake Tables (S3)
                │
                ▼
    📊 Analytics / Reporting Layer

---

## 🧠 Architecture Explanation

### 🧾 Source Layer
- Raw CSV files represent transactional e-commerce data
- Data is uploaded in batches

---

### ☁️ S3 Staging Layer
- Initial landing zone for raw files
- Organized by batch:

staging/batch_1/
staging/batch_2/


---

### 🔄 Airflow Orchestration (Astro on EC2)

Airflow manages the entire pipeline:

- 📦 Moves files from **staging → raw**
- ⏳ Waits for files using `S3KeySensor`
- 🚀 Triggers Databricks jobs
- 🔔 Sends success/failure notifications via SNS

---

### ⚡ Databricks Processing (ETL)

#### 🟤 Bronze Layer
- Raw ingestion
- Schema alignment
- No transformations

#### ⚪ Silver Layer
- Data cleaning
- Standardization
- Joins across tables

#### 🟡 Gold Layer
- Aggregations
- KPIs (sales, revenue, etc.)
- Analytics-ready data

---

### 🧊 Delta Lake
- Stores processed data in S3
- Supports:
- ACID transactions
- Schema evolution
- Time travel

---

### 📊 Analytics Layer
- Final datasets used for:
- Dashboards
- BI tools
- Business insights

---

## 🔄 Data Flow

1. Upload batch data → S3 staging
2. Airflow DAG starts
3. Move files → raw layer
4. Sensor waits for availability
5. Trigger Databricks:
 - Bronze → Silver → Gold
6. Send SNS notification
7. Data ready for analytics

---

## ⚙️ Tech Stack

| Layer | Tools |
|------|------|
| Orchestration | Apache Airflow (Astro + Docker) |
| Processing | Databricks (PySpark) |
| Storage | AWS S3 |
| Format | Delta Lake |
| Language | Python |
| Alerts | AWS SNS |
| Infra | AWS EC2 |

---
