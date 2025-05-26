# Covid-19-data-engineering-project
# 🦠 COVID-19 Data Engineering Project

This project is a modern data engineering pipeline designed to ingest, transform, and serve COVID-19 data using Microsoft Azure cloud services and Apache Spark.

---

## 📌 Project Objectives

- Build an end-to-end, scalable data platform using Azure-native tools.
- Ingest real-world COVID-19 datasets from public sources.
- Apply cleansing, enrichment, and transformation logic using both **ADF Data Flows** and **PySpark in Databricks**.
- Load transformed datasets into a **SQL Database** for analytics and visualization.

---

## 🚀 Architecture Overview


Public HTTP Sources
|
[ADF]
Copy Activities + Data Flow
|
Raw Zone (ADLS Gen2)
|
[Azure Databricks]
PySpark Transformations
|
Processed & Presentation Zone (Delta Tables in ADLS)
|
Azure SQL DB (via JDBC)
|
📊 Reporting / Power BI



---

## 🛠 Tech Stack

- **Azure Data Factory**: Pipeline orchestration, HTTP ingestion, Data Flows
- **Azure Data Lake Storage (Gen2)**: Raw, Processed, and Presentation zones
- **Azure Databricks**: PySpark-based data transformations and modeling
- **Delta Lake**: Versioned and partitioned table storage
- **Azure SQL Database**: Final data store for business consumption
- **Languages**: Python, SQL, PySpark

---

## 📂 Datasets

- COVID-19 Cases & Deaths (`cases_deaths.csv`)
- Hospital Admissions (`hospital_admissions.csv`)
- Population Demographics (`population.tsv`)
- Country Lookup (`country_lookup.csv`)

---

## ⚙️ Key Features

### ✅ Ingestion (ADF)
- Ingested `.csv` and `.tsv` files from public sources to the **Raw Layer** of ADLS.
- Used ADF **Copy Activity** and **parameterized pipelines** to handle multiple data types.
- Designed **Data Flows** to implement conditional splits and type conversions.

### 🧠 Transformation (Databricks + PySpark)
- Filtered and joined datasets across demographics, indicators, and country mappings.
- Created **weekly aggregates** (hospitalizations, ICU counts) and enriched data with **lookup tables**.
- Implemented **incremental loads**, partitioned writes, and timestamp tracking using `created_date`.

### 🧾 Serving (Azure SQL Database)
- Connected Databricks to Azure SQL via **JDBC**.
- Wrote cleaned and curated Delta tables to SQL tables (e.g., `dbo.DailyHospitalAdmissions`).
- Enabled reporting in Power BI via Azure SQL.

---

## 📊 Outcome

The project mimics a real-world data engineering environment, featuring:

- Hybrid **code-first** (Databricks) and **low-code** (ADF) pipelines
- Strong focus on **reusability, partitioning, and incremental loads**
- Demonstrated orchestration and transformation **across cloud services**

---



