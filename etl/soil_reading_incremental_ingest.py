import sqlite3
import requests
from datetime import datetime, timedelta
import logging
import time
import os

# ================== ABSOLUTE BASE ==================
BASE_DIR = r"C:\Users\eliza\smart_farming_analytics"

DB_FILE = os.path.join(BASE_DIR, "data", "soil_sensor_data.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

API_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/soil-sensor-readings-historical-data/records"
BATCH_SIZE = 100
MAX_RETRIES = 3
SLIDING_WINDOW_MINUTES = 2

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# ================== DB UTILS ==================
def get_last_timestamp(conn):
    cur = conn.execute("SELECT MAX(Local_Time) FROM raw_soil_data")
    row = cur.fetchone()
    if row and row[0]:
        return datetime.fromisoformat(row[0])
    return None

# ================== API ==================
def fetch_api_data(from_time=None):
    params = {"limit": BATCH_SIZE, "order_by": "local_time asc"}
    if from_time:
        params["where"] = f"local_time >= '{from_time.isoformat()}'"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("results", [])
        except Exception as e:
            logging.warning(f"API attempt {attempt} failed: {e}")
            time.sleep(2)

    logging.error("API failed after retries")
    return []

# ================== INGEST ==================
def incremental_ingest(conn, api_rows):
    inserted = 0

    for row in api_rows:
        try:
            cur = conn.execute("""
                INSERT OR IGNORE INTO raw_soil_data
                (ID, Local_Time, Site_Name, Site_ID, Probe_ID,
                 Probe_Measure, Soil_Value, Unit, json_featuretype)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(row["id"]),
                row["local_time"],
                row.get("site_name"),
                row.get("site_id"),
                row.get("probe_id"),
                row.get("probe_measure"),
                float(row["soil_value"]) if row.get("soil_value") else None,
                row.get("unit"),
                row.get("json_featuretype")
            ))

            if cur.rowcount == 1:
                inserted += 1

        except Exception as e:
            logging.error(f"Insert failed for ID {row.get('id')}: {e}")

    conn.commit()
    return inserted

# ================== MAIN ==================
def main():
    logging.info("===== ETL RUN STARTED =====")
    logging.info(f"DB_FILE = {DB_FILE}")
    logging.info(f"LOG_FILE = {LOG_FILE}")

    conn = sqlite3.connect(DB_FILE)

    last_dt = get_last_timestamp(conn)
    fetch_from = last_dt - timedelta(minutes=SLIDING_WINDOW_MINUTES) if last_dt else None

    logging.info(f"Last timestamp in DB: {last_dt}")
    logging.info(f"Fetching from: {fetch_from}")

    rows = fetch_api_data(fetch_from)

    if not rows:
        logging.info("No new rows fetched from API.")
        conn.close()
        logging.info("===== ETL RUN COMPLETED =====")
        return

    inserted = incremental_ingest(conn, rows)
    conn.close()

    logging.info(f"Rows inserted: {inserted}")
    logging.info("===== ETL RUN COMPLETED =====")

if __name__ == "__main__":
    main()
