
# 🌿 Smart Farming Analytics – A Data Engineering & Analytics Project

A data-focused project that collects and analyzes farm sensor data from an publically available API to derive insights for smarter irrigation decisions — combining time-series analysis, data pipeline design, and visualization.

---

## 📌 Project Objective

The goal of this project is to simulate a smart farm environment and build an end-to-end data analytics pipeline to:

- Collect real-time agricultural sensor data (e.g., soil moisture, soil temperature, soil salinity, Location meta data)
- Store and manage structured data efficiently
- Analyze patterns in time-series data to detect water stress
- Generate irrigation recommendations based on rule-based logic or simple predictive models
- Present insights in a clear, visual dashboard/report

---

## 💡 What This Project Demonstrates

Data Engineering Fundamentals: Streaming & batch data pipelines, ETL orchestration, structured storage.

Time-Series Analytics & Anomaly Detection: Detect soil moisture anomalies, track trends over time.

Modern Analytics Stack: Python (Pandas, NumPy), PySpark, scikit-learn (optional predictive modeling).

Dashboarding & Reporting: Streamlit or Power BI for clear, visual insights.

Enterprise-Ready Tools: Kafka, Snowflake, Delta Lake/HDFS, Airflow.

---

## 🧩 Planned Modules


![image](https://github.com/user-attachments/assets/8a7ef0be-4dc5-4fc6-b484-0f7cfc08071b)






## 🔧 Tech Stack


- **Python 3.11+**
- **Pandas** – data wrangling
- **NumPy** – numerical ops
- **matplotlib / seaborn** – visualizations
- **Streamlit** – optional dashboard
- **SQLite or CSV** – lightweight structured storage
- *(Optional later)* **scikit-learn** – basic prediction



| Layer              | Tools (Free / Community Versions)                    | Notes                                                     |
| ------------------ | ---------------------------------------------------- | --------------------------------------------------------- |
| **Data Ingestion** | Apache Kafka (open-source), Python scripts           | Simulate real-time sensor streaming locally.              |
| **Storage**        | DuckDB / SQLite / PostgreSQL                         | Lightweight, query-friendly, Parquet support.             |
| **Processing**     | PySpark (local mode), Python (Pandas/NumPy)          | Run locally; demonstrates ETL and large-scale processing. |
| **Orchestration**  | Apache Airflow (open-source), Prefect (free tier)    | Automate pipelines; show DAGs locally.                    |
| **Analytics & ML** | scikit-learn, statsmodels, Prophet                   | Rule-based and predictive irrigation recommendations.     |
| **Visualization**  | Streamlit (free), matplotlib, seaborn, Plotly (free) | Interactive dashboards and trend charts.                  |




---
## About the Data :: Why Simulated Sensor Data?

The dataset contains historical soil sensor readings (updated hourly) collected from public parks across the City of Melbourne. Sensors capture multiple environmental metrics like soil moisture, temperature, and salinity at varying depths. While the data originates from an urban setting, it was repurposed to mimic agricultural conditions for demonstrating data engineering and analytics capabilities.
