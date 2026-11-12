# Olist E-Commerce Data Engineering Platform

**End-to-end Data Engineering project | Status: In Progress**

## 1. Project Overview

This project aims to build a modern analytical data platform using the Brazilian Olist e-commerce dataset.

It covers data ingestion, batch processing, data transformation, orchestration, data quality, real-time streaming, cloud lakehouse technologies, CI/CD and business intelligence.

The project prioritizes free and locally executable technologies.

## 2. Architecture

```text
Olist CSV / External APIs / Synthetic Data
                  |
                  v
          Python Ingestion
                  |
                  v
       PostgreSQL / MinIO
                  |
                  v
          Apache Spark
                  |
                  v
       Delta Lake (Bronze)
                  |
                  v
       Delta Lake (Silver)
                  |
                  v
        Delta Lake (Gold)
                  |
                  v
             dbt Core
                  |
                  v
        Analytical Models
                  |
                  v
           Power BI (Planned)

Orchestration: Apache Airflow

Streaming:
Python Producer -> Kafka -> Spark Structured Streaming -> Delta Lake

Cloud Lakehouse: Databricks Free Edition (Planned)
CI/CD: GitHub Actions (Planned)
```

## 3. Technology Stack

| Area | Technologies |
|---|---|
| Programming | Python, SQL |
| Database | PostgreSQL |
| Storage | MinIO, Parquet |
| Big Data | Apache Spark, PySpark |
| Lakehouse | Delta Lake |
| Analytics Engineering | dbt Core |
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Cloud Lakehouse | Databricks Free Edition |
| CI/CD | Git, GitHub Actions |
| Testing | Pytest, dbt tests, Great Expectations |
| Containers | Docker, Docker Compose |
| BI | Power BI Desktop |

## 4. Implemented Features

- Olist CSV ingestion into PostgreSQL.
- Data warehouse development.
- SQL transformations and dbt models.
- PySpark batch processing.
- Bronze, Silver and Gold architecture.
- Delta Lake storage and transformations.
- Airflow DAG orchestration.
- Eight data quality validation checks.
- Python Kafka event producer.
- Spark Structured Streaming consumer.
- Event deduplication and Delta MERGE.
- Streaming data validation.
- Kafka technical documentation.

## 5. Data Quality Results

The analytical fact table contains **112,650 records**.

Eight validation checks passed:

- Required values are not missing.
- No duplicate order-item records.
- No negative product prices.
- No negative freight values.
- No missing customer dimension keys.
- No missing product dimension keys.
- No missing seller dimension keys.
- No missing date dimension keys.

## 6. Streaming Architecture

```text
Python Producer
      |
      v
Kafka Topic: orders
      |
      v
Spark Structured Streaming
      |
      v
JSON Parsing and Validation
      |
      v
Event Deduplication
      |
      v
Delta Lake MERGE
```

## 7. Databricks Free Edition

Planned activities:

- Explore Databricks Workspace and Notebooks.
- Run PySpark transformations.
- Work with Delta Lake.
- Explore Jobs and Workflows.
- Explore Unity Catalog where available.

Databricks will complement the local Spark environment.

## 8. CI/CD with GitHub Actions

Planned activities:

- Automate Python tests with Pytest.
- Validate dbt models and tests.
- Check code quality with Ruff.
- Create GitHub Actions workflows.
- Manage sensitive configuration using GitHub Secrets.
- Automate validation on every push and pull request.

## 9. Project Roadmap

- [x] Python, SQL and PostgreSQL foundations
- [x] Data warehouse and dbt transformations
- [x] PySpark and Delta Lake
- [x] Bronze-Silver-Gold architecture
- [x] Airflow orchestration
- [x] Eight data quality checks
- [x] Kafka streaming implementation
- [x] Kafka technical documentation
- [ ] Kafka restart and recovery testing
- [ ] MinIO integration
- [ ] External API and synthetic data enrichment
- [ ] Databricks Free Edition
- [ ] CI/CD with GitHub Actions
- [ ] Advanced automated testing
- [ ] Power BI dashboard
- [ ] Final technical documentation

## 10. Repository Structure

```text
data-eng-project/
├── dags/
├── docs/
├── ingestion/
├── monitoring/
├── olist_analytics/
├── quality/
├── spark_jobs/
├── src/
├── streaming/
├── tests/
├── .gitignore
├── run_pipeline.py
└── README.md
```

## 11. Execution Environment

The core pipeline is designed to run locally using free and open-source technologies.

Cloud services are optional learning environments and are not required to execute the local pipeline.

Detailed setup instructions and execution commands will be documented as the project progresses.

## 12. Author

**Field:** Data Science and Data Engineering

**Project:** Olist E-Commerce Data Engineering Platform
