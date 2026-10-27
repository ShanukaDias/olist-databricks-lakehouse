Markdown
# 🛒 Olist E-Commerce Data Lakehouse (Databricks & PySpark)

An end-to-end Data Lakehouse solution built on **Databricks** utilizing the **Medallion Architecture (Bronze → Silver → Gold)**. This pipeline ingests raw relational e-commerce dataset files (100k+ records), performs data validation, executes multi-table relational joins, and produces aggregated business metrics exposed via an executive dashboard.

---

## 🏗️ Architecture Overview

The pipeline follows modern Lakehouse design principles:

[ Raw CSV Files ]
│
▼
[ Bronze Layer ]  ──► Raw Ingestion & Schema Preservation (Delta Lake)
│
▼
[ Silver Layer ]  ──► Data Quality Filtering, Type Casting & Relational Joins
│
▼
[ Gold Layer ]    ──► Business Aggregations (Daily Sales & Regional Summary)
│
▼
[ Dashboard ]     ──► Executive Visualization & Reporting


* **Bronze (Raw Ingestion):** Reads multi-table raw e-commerce CSV datasets, attaches ingestion metadata timestamps, and stores them in native **Delta Lake** tables.
* **Silver (Cleaned & Joined):** Filters completed/delivered orders, handles missing/null primary keys, casts data types accurately, and joins `orders`, `order_items`, and `customers`.
* **Gold (Business Ready):** Computes aggregated business metrics (Daily Revenue, Average Order Value, Freight-to-Price Ratios, and Revenue by State).

---

## 🛠️ Tech Stack & Engineering Highlights

* **Platform:** Databricks Community Edition / Serverless Compute
* **Storage Engine:** Delta Lake (ACID Transactions, Schema Enforcement, Time Travel)
* **Processing Framework:** PySpark / Spark SQL
* **Orchestration:** Databricks Workflows (Sequential Multi-Task Job Execution)
* **Governance:** Unity Catalog (Volumes & Schemas)
* **Visualization:** Databricks AI/BI Executive Dashboard

---

## ⚙️ Workflow Automation & Monitoring

The entire pipeline is automated using **Databricks Workflows (Jobs)**. Tasks run sequentially with strict dependency management (`bronze_ingestion` → `silver_transform` → `gold_aggregations`).


---

## 📊 Executive Dashboard

The final Gold tables power an executive dashboard tracking high-level KPIs, daily revenue trends, and regional customer concentration across Brazilian states.

---

## 💡 Key Engineering Trade-offs & Decisions

1. Delta Lake over Standard Parquet: Delta Lake was chosen to provide ACID guarantees during overwrites, schema evolution support, and time-travel querying capabilities.
2. Relational Join Strategy: Joining order lines with customer location data in the Silver layer reduces redundant computation across multiple downstream Gold tables.
3. Data Quality Assertions: Implemented strict row filtering (e.g., restricting analysis to `order_status = 'delivered'`) to maintain data integrity for downstream reporting.

---

## 🚀 How to Run

1. Clone this repository.
2. Upload the Olist dataset files to your Databricks Volume path (`/Volumes/<catalog>/<schema>/raw_data/`).
3. Import the Python scripts in `notebooks/` into your Databricks workspace.
4. Execute `01_bronze_ingestion.py`, `02_silver_transform.py`, and `03_gold_aggregations.py` seque
