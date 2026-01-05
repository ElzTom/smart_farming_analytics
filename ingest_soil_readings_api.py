import requests
import sqlite3
import time
from etl.config_loader import load_config

# Load config
config = load_config()

DB_FILE = config["database"]["db_file"]
SENSOR_API_URL = config["api"]["sensor_data_url"]
BATCH_SIZE = config["api"]["batch_size"]
MAX_RETRIES = config["api"]["max_retries"]
TIMEZONE = config["api"]["timezone"]


def sanitize_text(value):
    if value is None:
        return None
    return value.replace("#", " ") if isinstance(value, str) else value


def get_last_date(conn):
    cursor = conn.execute("SELECT MAX(Local_Time) FROM raw_soil_data")
    last = cursor.fetchone()[0]
    return last or "2000-01-01T00:00:00+00:00"


def fetch_sensor_data(last_date):
    params = {
        "limit": BATCH_SIZE,
        "order_by": "local_time asc",
        "timezone": TIMEZONE,
        "where": f"local_time>'{last_date}'"
    }

    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(SENSOR_API_URL, params=params)
        if response.status_code == 200:
            return response.json().get("results", [])
        retries += 1
        time.sleep(2)

    print("Failed after max retries.")
    return []


def insert_sensor_data(conn, records):
    with conn:
        for rec in records:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO raw_soil_data
                    (ID, Local_Time, Site_Name, Site_ID, Probe_ID, Probe_Measure, Soil_Value, Unit, json_featuretype)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec.get("id"),
                    rec.get("local_time"),
                    sanitize_text(rec.get("site_name")),
                    rec.get("site_id"),
                    rec.get("probe_id"),
                    sanitize_text(rec.get("probe_measure")),
                    rec.get("soil_value"),
                    sanitize_text(rec.get("unit")),
                    sanitize_text(rec.get("json_featuretype"))
                ))
            except Exception as e:
                print(f"Error inserting record {rec.get('id')}: {e}")


def main():
    conn = sqlite3.connect(DB_FILE)
    last_date = get_last_date(conn)

    print(f"Starting incremental fetch from {last_date}")
    total_inserted = 0

    while True:
        batch = fetch_sensor_data(last_date)
        if not batch:
            break

        insert_sensor_data(conn, batch)
        total_inserted += len(batch)
        last_date = batch[-1]["local_time"]

        print(f"Inserted {total_inserted} records so far...")
        time.sleep(0.5)

    conn.close()
    print(f"Completed! Inserted {total_inserted} new records.")


if __name__ == "__main__":
    main()
