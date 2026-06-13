# Apache Airflow Portfolio — 10 Professional Data Pipelines

A comprehensive, production-grade portfolio showcasing **10 diverse Apache Airflow DAGs** covering the full spectrum of Data Engineering, MLOps, and Analytics.

Built with: **Apache Airflow** | **Google BigQuery** | **dbt** | **MLflow** | **Apache Spark** | **Astronomer Cosmos** | **Python**

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Orchestrator)             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  01 Weather Alert     ──→  OpenWeatherMap API + Branching    │
│  02 Crypto Tracker    ──→  CoinGecko API + Time-Series       │
│  03 Wiki Trends       ──→  Wikimedia API + Trend Analysis    │
│  04 Medallion Arch.   ──→  Bronze/Silver/Gold (Pandas)       │
│  05 Log Analytics     ──→  Log Generation + Anomaly Detect   │
│  06 dbt Warehouse     ──→  Astronomer Cosmos + Datasets      │
│  07 Spark Big Data    ──→  PySpark Window Functions          │
│  08 MLOps MLflow      ──→  ML Training + Model Registry      │
│  09 Event Kafka       ──→  Streaming Simulation + Funnel     │
│  10 Meta Monitoring   ──→  Self-Monitoring + SLA Alerting    │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Data Lake: GCS  │  Data Warehouse: BigQuery  │  MLflow UI   │
└──────────────────────────────────────────────────────────────┘
```

## 📂 Project Structure

```
airflow-portfolio/
├── dags/
│   ├── 01_weather_alert/       # API extraction + conditional alerting
│   ├── 02_crypto_tracker/      # Crypto time-series pipeline
│   ├── 03_wiki_trends/         # Wikipedia trending topics
│   ├── 04_ecommerce_medallion/ # Medallion Architecture (Bronze/Silver/Gold)
│   ├── 05_log_analytics/       # Log parsing + anomaly detection
│   ├── 06_dbt_warehouse/       # Astronomer Cosmos + Data-Aware Scheduling
│   ├── 07_spark_bigdata/       # PySpark distributed processing
│   ├── 08_mlops_mlflow/        # ML lifecycle with MLflow tracking
│   ├── 09_event_kafka/         # Kafka-like event stream analytics
│   ├── 10_meta_monitoring/     # Platform self-monitoring
│   └── ecommerce_pipeline_dag.py  # EL pipeline (GCS → BigQuery)
├── dbt_project/                # dbt models (staging + marts)
├── plugins/
│   ├── data_generator/         # Faker-based synthetic data
│   └── ml_pipeline/            # scikit-learn + MLflow training
├── docker-compose.yaml         # Full stack: Airflow + Redis + Postgres + MLflow
├── requirements.txt
└── .env.example
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- (Optional) Google Cloud Platform account for DAGs 06 and the EL pipeline

### Setup
```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/airflow-portfolio.git
cd airflow-portfolio

# 2. Configure environment
cp .env.example .env
# Edit .env with your GCP project ID and bucket name (if using GCP)

# 3. Start all services
docker compose up -d

# 4. Access the UIs
# Airflow:  http://localhost:8080  (user: airflow, pass: airflow)
# MLflow:   http://localhost:5000
```

## 📊 DAG Descriptions

| # |        DAG         | Schedule        | Key Skills Demonstrated |
|---|--------------------|-----------------|------------------------|
| 01 | Weather Alert      | `@hourly`       | REST API, `BranchPythonOperator`, XCom, conditional logic |
| 02 | Crypto Tracker     | `@daily`        | CoinGecko API, time-series data, historical appending |
| 03 | Wiki Trends        | `@daily`        | Wikimedia API, text filtering, trend analysis |
| 04 | E-Commerce Medallion | `@daily`        | Medallion Architecture, Pandas, Parquet, data quality |
| 05 | Log Analytics      | `@daily`        | Log generation (Faker), regex parsing, anomaly detection |
| 06 | dbt Warehouse      | Dataset-triggered | Astronomer Cosmos, Data-Aware Scheduling, Star Schema |
| 07 | Spark Big Data     | `@weekly`       | PySpark, Window Functions, distributed computing, Parquet |
| 08 | MLOps MLflow       | `@weekly`       | scikit-learn, MLflow autolog, model registry, evaluation |
| 09 | Event Kafka        | `@daily`        | Kafka-like simulation, JSON Lines, conversion funnel |
| 10 | Meta Monitoring    | `@hourly`       | Airflow metadata DB, SLA monitoring, platform health |

## 🛠️ Tech Stack

- **Orchestration:** Apache Airflow 2.9
- **Data Warehouse:** Google BigQuery
- **Data Lake:** Google Cloud Storage
- **Transformation:** dbt + Astronomer Cosmos
- **ML Tracking:** MLflow
- **Big Data:** Apache Spark (PySpark)
- **Data Generation:** Faker
- **Containerization:** Docker Compose

## 📝 License

This project is for portfolio/educational purposes.

## Testing
```bash
# Install pytest and pytest-mock
pip install pytest pytest-mock

# Run tests
pytest
```

## Troubleshooting
```bash
# List all DAGs
airflow dags list

# Trigger a DAG
airflow dags trigger <dag_id>

# List tasks in a DAG
airflow tasks list <dag_id>

# Run a task
airflow tasks test <dag_id> <task_id> <execution_date>

# View DAG logs
airflow dags logs <dag_id> <execution_date>
```

## Production Features
 This is a professional Airflow portfolio with production-grade features like:
 - Data-aware scheduling with Dataset
 - dbt integration with Astronomer Cosmos
 - MLflow integration for ML model tracking
 - Google Cloud Storage for data lake
 - Google BigQuery for data warehouse
 - Airflow metadata DB monitoring
 - SLA monitoring
 - Platform health monitoring
 
 ## Portfolio Projects
 * I have added 10 DAGs to showcase my skills in Data Engineering, MLOps, and Analytics:
 - Weather Alert DAG: API extraction + conditional alerting
 - Crypto Tracker DAG: Time-series data + historical appending
 - Wiki Trends DAG: Wikimedia API + trend analysis
 - E-Commerce Medallion DAG: Medallion Architecture + Pandas
 - Log Analytics DAG: Log generation + anomaly detection
 - dbt Warehouse DAG: Astronomer Cosmos + Data-Aware Scheduling
 - Spark Big Data DAG: PySpark + Window Functions
 - MLOps MLflow DAG: ML training + model registry
 - Event Kafka DAG: Streaming simulation + conversion funnel
 - Meta Monitoring DAG: Self-monitoring + SLA alerting
 ```

 ## Cloud Setup
 This project includes a cloud setup for local development:
 ```bash
 docker compose up -d
 ```

 ## dbt Project
 ```bash
 dbt --project-dir dbt_project --profile ecommerce run
 ```


## Testing
```bash
# Install pytest and pytest-mock
pip install pytest pytest-mock

# Run tests
pytest
```

