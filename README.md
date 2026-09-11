# End-to-End Cloud Data & AI Engine (GCP + Databricks + PySpark)

An enterprise-grade Lakehouse data pipeline and natural language query engine for crypto market analytics, built on GCP Storage and Databricks Delta Lake.

## 🏗️ Architecture
1. **Ingestion Layer (Bronze):** Python script executing REST API ingestion directly to **GCP Cloud Storage** via Cloud Shell CLI.
2. **Processing Layer (Silver):** Distributed data transformation, schema enforcement, and cleansing using **PySpark** in Databricks. Saved as Delta Lake tables.
3. **Analytics Layer (Gold):** Business aggregation, sentiment classification, and star-schema modeling for downstream consumption.
4. **AI Layer:** Text-to-SQL execution engine transforming natural language prompts into automated Spark SQL queries against Delta tables.

## 🛠️ Tech Stack
* **Cloud Platform:** Google Cloud Platform (GCS, Cloud Shell)
* **Data Processing & Storage:** Databricks, Apache Spark (PySpark), Delta Lake
* **Languages:** Python, SQL
* **AI Integration:** LLM Text-to-SQL logic pattern

## 🚀 How to Run
1. Execute `ingest.py` in GCP Cloud Shell to land raw payload in GCS bucket.
2. Run `01_gold_silver_pipeline.ipynb` in Databricks to transform raw JSON to Silver/Gold Delta tables.
3. Run `ai_query_engine()` to query data using natural language prompts.
