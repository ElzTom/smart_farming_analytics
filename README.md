# Smart Farming Analytics

An end-to-end data engineering project that ingests IoT soil sensor data from a public API, processes it through a medallion pipeline, and surfaces irrigation insights in a live dashboard.

---

## What This Project Does

Pulls soil moisture, temperature, and salinity readings from the City of Melbourne's public sensor API (70+ park sites), runs them through a Bronze → Silver → Gold pipeline, loads the results into PostgreSQL, and displays irrigation recommendations in Grafana.

The pipeline runs automatically on a daily schedule via Apache Airflow. When the API publishes new data, everything updates without manual intervention.

---

## Architecture

```
Melbourne Sensor API
        |
        v
   Bronze Layer        Raw Parquet files (ingested as-is)
        |
        v
   Silver Layer        Cleaned, typed, deduplicated
        |
        v
    Gold Layer         Daily site summaries + irrigation flags
        |
        v
   PostgreSQL          Queryable gold tables
        |
        v
    Grafana            Live dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker, LocalExecutor) |
| Processing | PySpark (local mode) |
| Storage | Parquet (bronze/silver/gold) + PostgreSQL |
| Containerisation | Docker Compose |
| Dashboard | Grafana |
| Language | Python 3.12 |

---

## Key Outputs

**daily_site_summary** — one row per site per day:
- Soil moisture by depth (10cm–80cm)
- Shallow and deep average moisture
- Soil temperature and salinity
- Weather join (rainfall, max temp, solar exposure)
- `irrigation_needed` flag: shallow moisture < 30% AND rainfall < 2mm

**hourly_site_summary** — same metrics at hourly grain for trend analysis.

---

## Dashboard

Grafana at `http://localhost:3000`:
- **Irrigation Status** — table of all sites for the latest date, ordered by driest soil first, color-coded by irrigation need
- **Soil Moisture Trend** — time series of the 5 driest sites with a threshold line at 30%

---

## Running Locally

**Requirements:** Docker Desktop, WSL2

```bash
# Start the full stack
docker compose up -d

# Airflow UI
http://localhost:8080  (admin / admin)

# Grafana
http://localhost:3000  (admin / admin)

# Trigger pipeline manually
# Go to Airflow UI → smart_farming_pipeline → trigger
```

The DAG runs automatically at 6am daily.

---

## About the Data

Soil sensor readings from public parks across the City of Melbourne. Sensors record moisture, temperature, and salinity at depths of 10cm–80cm. Data is available via the City of Melbourne Open Data API and was used here to demonstrate a production-style data engineering pipeline.

Data covers mid-2024 to early 2026.
