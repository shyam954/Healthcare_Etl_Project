🏥 Healthcare ETL Pipeline with PySpark, AWS, Airflow & Docker

📌 Project Overview
This project implements a production-grade Healthcare ETL pipeline using PySpark, AWS S3, Airflow, and Docker.
It simulates real-world healthcare data, processes it through Bronze → Silver → Gold layers, and produces analytics-ready datasets and KPIs using a modular, scalable architecture.
The pipeline is fully containerized using Docker and orchestrated with Apache Airflow, making it deployment-ready and cloud-friendly.

🧰 Tech Stack & Tools
Programming Language: Python
Big Data Processing: PySpark
Workflow Orchestration: Apache Airflow
Cloud Storage: AWS S3
Containerization: Docker & Docker Compose
Data Generation: Faker
Configuration Management: JSON, .env
Storage Format: Parquet
🏗️ High-Level Architecture
Airflow DAG
    |
    ▼
Main Orchestration Script
    |
    ▼
Spark Session Initialization
    |
    ▼
Synthetic Data Generation (Faker)
    |
    ▼
Bronze Layer (Raw Ingestion)
    |
    ▼
Data Validation
    |
    ▼
Silver Layer (Cleaned & Standardized)
    |
    ▼
Gold Layer (Facts & Dimensions)
    |
    ▼
Analytics & KPIs
    |
    ▼
Write to S3 / Local Storage
📂 Project Structure
healthcare-etl-pyspark/
│
├── dags/                         # Airflow DAGs
├── src/
│   ├── spark_jobs/
│   │   ├── bronze_level/         # Data ingestion
│   │   ├── silver_level/         # Data cleaning
│   │   └── gold_level/           # Analytics & KPIs
│   └── utils/                    # Spark session, helpers
│
├── main_pipeline_orchestration.py
├── health_careEtl.json           # Config file
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .env
└── README.md

🔄 ETL Pipeline Layers
🥉 Bronze Layer – Raw Ingestion
Ingests raw CSV healthcare data:
Patients
Encounters
Treatments
Adds metadata:
Ingestion timestamp
Batch ID
Source file name
🥈 Silver Layer – Data Cleaning & Validation
Removes duplicates
Handles null values
Enforces foreign-key relationships
Standardizes categorical fields
Adds derived columns (e.g., billable flag)

🥇 Gold Layer – Analytics & KPIs
Dimension Tables
dim_patient
dim_doctor
dim_hospital_unit
dim_date
Fact Tables
fact_encounters
fact_treatments

📊 Analytics & Insights Extracted
From the Gold layer, the pipeline generates:
Patient demographics & registration trends
Hospital visit analysis
Encounters per department
Encounter type distribution
Treatment analytics
Billable vs non-billable treatments
Treatment cost aggregation
Operational KPIs
Encounter cancellation rate
Revenue loss due to cancellations
Total treatment revenue
Healthcare operational insights
Department-level workload analysis
Financial performance indicators

⏱️ Orchestration with Airflow
Airflow DAG schedules and monitors the ETL pipeline
Handles:
Job retries
Failure alerts
Dependency management
Enables fully automated batch processing

🐳 Dockerized Deployment
Docker ensures:
Consistent runtime environment
Easy local & cloud deployment
Docker Compose spins up:
Airflow services
Spark environment
Supporting infrastructure

▶️ How to Run the Project
1️⃣ Clone Repository
git clone https://github.com/your-username/healthcare-etl-pyspark.git
cd healthcare-etl-pyspark
2️⃣ Configure Environment
Update .env with:
AWS credentials
Environment variables
3️⃣ Start Services
docker-compose up -d
4️⃣ Trigger Pipeline
Open Airflow UI
Enable DAG
Trigger ETL run
🚀 Key Highlights
Production-grade modular architecture
Clear Bronze / Silver / Gold layering
Airflow-driven orchestration
Dockerized & cloud-ready
Realistic healthcare analytics use cases
