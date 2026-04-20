# 🛒 E-Commerce Analytics Platform (End-to-End Data Engineering Project)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red)
![Databricks](https://img.shields.io/badge/Databricks-ETL-orange)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20SNS-yellow)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-green)

---

## 📌 Overview

This project implements a **production-style data engineering pipeline** for an e-commerce platform using:

- Batch processing (incremental loads)
- Lakehouse architecture (Bronze → Silver → Gold)
- Workflow orchestration with Airflow (Astro + Docker)
- Cloud storage (AWS S3)
- Databricks for distributed processing

---

## 🏗️ Architecture

         ┌──────────────┐
         │  Source Data │
         │ (CSV files)  │
         └──────┬───────┘
                │
                ▼
    ┌──────────────────────┐
    │   S3 (Staging Area)  │
    └─────────┬────────────┘
              │
              ▼
    ┌──────────────────────┐
    │ Airflow (Astro + EC2)│
    │  - Move Files        │
    │  - Sensors           │
    │  - Trigger Jobs      │
    └─────────┬────────────┘
              │
              ▼
    ┌──────────────────────┐
    │ Databricks (ETL)     │
    │ Bronze → Silver → Gold│
    └─────────┬────────────┘
              │
              ▼
    ┌──────────────────────┐
    │   Delta Lake Tables  │
    └─────────┬────────────┘
              │
              ▼
    ┌──────────────────────┐
    │   Analytics Layer    │
    │ (Reporting / BI)     │
    └──────────────────────┘

---

## 🔄 Data Flow

1. Raw data arrives in **S3 staging bucket**
2. Airflow DAG:
   - Moves files → raw layer
   - Waits using `S3KeySensor`
3. Airflow triggers **Databricks jobs**
4. Data is processed through:
   - Bronze (raw ingestion)
   - Silver (cleaned data)
   - Gold (business metrics)
5. Notifications sent via **SNS**
6. Final data ready for analytics

---

## ⚙️ Tech Stack

| Layer | Tools |
|------|------|
| Orchestration | Apache Airflow (Astro CLI + Docker) |
| Processing | Databricks (PySpark) |
| Storage | AWS S3 |
| Data Format | Delta Lake |
| Language | Python |
| Notifications | AWS SNS |
| Infrastructure | AWS EC2 |

---

## 📂 Project Structure


E-Commerce-Analytics-Platform/
│
├── dags/
│ ├── batch_pipeline.py
│
├── notebooks/
│ ├── run_bronze.py
│ ├── run_silver.py
│ ├── run_gold.py
│
├── airflow-docker/
│ ├── dags/
│ ├── Dockerfile
│ ├── requirements.txt
│
├── config/
│ ├── table_config.py
│
└── README.md


---

## 🚀 Airflow DAG (OOP Design)

- Implemented using class-based design
- Parameterized execution using:

```json
{
  "batch_number": "2"
}
DAG Flow
move_s3 → S3 sensor → Bronze → Silver → Gold → Notify
