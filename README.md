# 🏥 End-to-End Azure Lakehouse & Orchestration Pipeline

[![Azure](https://img.shields.io/badge/Azure-Data%20Factory%20%7C%20Databricks%20%7C%20Synapse-0089D6?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Parquet-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)

An enterprise-grade, production-ready Azure Medallion Lakehouse data architecture designed to process, model, and serve high-volume healthcare and hospitalization operational metrics. Built with **Apache Airflow**, **Azure ADLS Gen2**, **Azure Databricks (PySpark)**, **Azure Synapse Analytics**, and **Power BI**, with full CI/CD deployment via **GitHub Actions**.

---

## 🎯 Executive Summary & Business Problem

Healthcare systems and public health agencies manage millions of daily operational records covering hospital admissions, ICU capacity, Referral-to-Treatment (RTT) waiting times, and patient throughput. Processing these metrics presents critical enterprise data engineering challenges:

* **Data Fragmentation & Inconsistency**: Ingesting disparate REST APIs and unstructured batch feeds often leads to schema drift, missing date fields, and duplicate reporting records.
* **Scalability Bottlenecks**: Legacy data warehouses struggle to execute real-time and heavy batch transformations across multi-gigabyte historical healthcare datasets.
* **Data Trust & Governance**: BI teams and clinical stakeholders require fully validated, audit-tracked data with strict Data Quality (DQ) assertions before executive dashboard consumption.

### 💡 The Solution

This pipeline implements an enterprise **Medallion Architecture (Bronze → Silver → Gold)** to decouple raw data ingestion from analytical data serving:
1. **Bronze Layer**: Ingests multi-source raw healthcare streams via Apache Airflow DAGs into Azure Data Lake Storage Gen2, appending audit metadata.
2. **Silver Layer**: Leverages PySpark on Azure Databricks to clean, standardize dates, deduplicate entities, and apply automated Data Quality assertions.
3. **Gold Layer**: Aggregates structured data into Dimensional Star-Schema models (`FactHealthcareSummary`, `DimLocation`, `DimMetric`) exposed via Azure Synapse SQL Endpoints and Power BI dashboards.

---

## 🏗️ End-to-End Architecture & Workflow
<img width="1033" height="593" alt="image" src="https://github.com/user-attachments/assets/33f745a0-e7fb-42fe-af7a-ff345afe99b0" />

---

## 🛠️ Technology Stack & Tools

* **Orchestration**: Apache Airflow (`/dags`), Azure Data Factory (ADF) triggers.
* **Compute Engine**: Azure Databricks (Apache Spark / PySpark 3.x).
* **Storage & Lakehouse**: Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake / Parquet.
* **Data Warehouse & Serving**: Azure Synapse Analytics (Serverless & Dedicated SQL Pools).
* **Data Quality & Governance**: Great Expectations & PySpark Dataset Assertions.
* **CI/CD & DevOps**: GitHub Actions (`.github/workflows/ci-cd.yml`), Terraform IaC.
* **Visualization**: Power BI Enterprise KPI Dashboards.

---

## 📁 Repository Directory Structure

```text
azure-medallion-lakehouse-pipeline/
├── .github/
│   └── workflows/
│       └── ci-cd.yml                  # Automated build, test, and validation workflow
├── dags/
│   ├── nhs_rtt_ingestion_dag.py       # Apache Airflow DAG for scheduled API ingestion
│   └── utils/
│       └── alerts.py                  # Failure notifications & webhook callback utilities
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Raw ingestion with metadata tag enrichment
│   ├── 02_silver_cleaning.py           # PySpark transformations & Data Quality checks
│   └── 03_gold_transform.py           # Star schema aggregates & metric calculations
├── sql/
│   └── synapse/
│       └── create_tables.sql          # External tables, dimensions, and fact schemas
├── data/                              # Lightweight local git-tracked samples (Large datasets ignored)
│   ├── bronze/ .gitkeep
│   ├── silver/ .gitkeep
│   └── gold/ .gitkeep
├── tests/
│   └── test_pipeline.py               # Unit testing suite for PySpark transformations
├── docs/
│   └── architecture_diagram.png       # Visual architecture assets
├── .gitignore                         # Excludes heavy raw binaries (>100MB)
├── requirements.txt                   # Python & PySpark dependencies
└── README.md                          # Repository documentation

```
## ⚡ Pipeline Implementation Details

### 🥉 1. Bronze Layer (Raw Ingestion)

- Ingests real-time and batch feeds directly from public APIs and raw CSV/JSON streams.
- Appends immutable operational metadata to every record:
  - `_ingestion_timestamp`: UTC execution timestamp.
  - `_source_system`: Origin system tracking identifier.

 ### 🥈 2. Silver Layer (Cleansing & Quality Rules)

- Enforces strict schema definitions and standardizes date formatting (`YYYY-MM-DD`).
- Removes duplicate primary key records across entities and observation timestamps.
- Runs automated PySpark Data Quality rules to handle nulls and invalid metrics:

```python
from pyspark.sql import functions as F

df_silver = df_bronze \
    .filter(F.col("entity").isNotNull()) \
    .withColumn(
        "value",
        F.when(F.col("value") < 0, None).otherwise(F.col("value"))
    ) \
    .withColumn(
        "dq_value_flag",
        F.when(
            F.col("value").isNull(),
            "INVALID_VALUE"
        ).otherwise("VALID")
    )
```

### 🥇 3. Gold Layer (Dimensional Star Schema)

- Models clean data into dimensional structures for analytical querying:
  - `FactHealthcareSummary`: Key business measures (hospital admission counts, weekly ICU averages, bed occupancy metrics).
  - `DimLocation`: Geographic and administrative hierarchies (country, health region, ISO code).
  - `DimMetric`: Indicator definitions, reporting categories, and units of measure.

## 🧪 Data Quality & Alerting Framework

Pipeline reliability is enforced using automated unit testing and real-time failure alerting:

- **PySpark Unit Testing**: Executes automated transformation tests using `pytest` inside GitHub Actions CI/CD workflows on every pull request or main branch merge.
- **Airflow Failure Callbacks**: Integrates `on_failure_callback` hooks to issue instant alerts (Slack / Teams / Email webhooks) if an ingestion SLA or quality constraint fails.

## 🚀 How to Run & Deploy

### Prerequisites

- Python 3.10+
- Apache Spark / PySpark 3.x
- Azure CLI & Databricks CLI

## 📊 Analytics & BI Dashboards

Gold layer analytical tables directly power executive Power BI dashboards and Synapse SQL endpoints:

- **Hospital Throughput**: Monitors admission velocity, bed occupancy saturation rates, and ICU workload trends.
- **Regional Comparatives**: Enables multi-region performance comparisons across healthcare providers.
- **Data Quality Tracking**: Live telemetry tracking ingestion freshness and percentage of flagged invalid records.

## 👤 Author

**Sejal Gohil**

- **Role**: Data Engineer
- **GitHub**: [@sejal-gohil09](https://github.com/sejal-gohil09)
- **Project**: [End-to-End Azure Lakehouse & Orchestration Pipeline](https://github.com/sejal-gohil09/End-to-End-Azure-Lakehouse-Orchestration-Pipeline)


Azure CLI & Databricks CLI

