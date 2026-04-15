# 🏦 Banking Data Engineering Project

---

## 📌 Project Overview
A **full end-to-end data pipeline** built for the Banking domain. This project simulates customer, account, and transaction data, streams changes in real time, 
transforms them into analytics-ready models, and visualizes key insights — all following 
CI/CD and data warehousing best practices.

---

## 🏗️ Architecture  

**Pipeline Flow:**
1. **Data Generator** → Simulates banking transactions, accounts & customers (via Faker).  
2. **Kafka + Debezium** → Streams change data (CDC) into MinIO (S3-compatible storage).  
3. **Airflow** → Orchestrates data ingestion & snapshots into Snowflake.  
4. **Snowflake** → Cloud Data Warehouse (Bronze → Silver → Gold).  
5. **DBT** → Applies transformations, builds marts & snapshots (SCD Type-2).  
6. **CI/CD with GitHub Actions** → Automated tests, build & deployment.  

---

## ⚡ Tech Stack
- **Snowflake** → Cloud Data Warehouse  
- **DBT** → Transformations, testing, snapshots (SCD Type-2)  
- **Apache Airflow** → Orchestration & DAG scheduling  
- **Apache Kafka + Debezium** → Real-time streaming & CDC  
- **MinIO** → S3-compatible object storage  
- **Postgres** → Source OLTP system  
- **Python (Faker)** → Data simulation  
- **Docker & docker-compose** → Containerized setup  

---
