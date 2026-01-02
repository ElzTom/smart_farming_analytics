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


def fetch_sensor_data(limit, offset=0):
    params = {
        "limit": limit,
        "offset": offset,
        "order_by": "local_time asc",
        "timezone": TIMEZONE
    }
    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(SENSOR_API_URL, params=params)
        if response.status_code == 200:
            return response.json().get("results", [])
        else:
            print(f"Request failed ({response.status_code}). Retrying ({retries + 1})...")
            retries += 1
            time.sleep(2)
    print("❌ Failed after max retries.")
    return []


def insert_sensor_data(conn, records):
    with conn:
        for rec in records:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO raw_soil_data 
                    (id, local_time, site_name, site_id, probe_id, probe_measure, soil_value, unit, json_featuretype)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    rec.get("id"),
                    rec.get("local_time"),
                    rec.get("site_name"),
                    rec.get("site_id"),
                    rec.get("probe_id"),
                    rec.get("probe_measure"),
                    rec.get("soil_value"),
                    rec.get("unit"),
                    rec.get("json_featuretype")
                ))
            except Exception as e:
                print(f"Error inserting record {rec.get('id')}: {e}")


def main():
    conn = sqlite3.connect(DB_FILE)
    total_inserted = 0
    offset = 0

    while True:
        batch = fetch_sensor_data(BATCH_SIZE, offset=offset)
        if not batch:
            break

        insert_sensor_data(conn, batch)
        total_inserted += len(batch)
        print(f"📥 Inserted {total_inserted} records so far...")

        if len(batch) < BATCH_SIZE:
            break  # No more records

        offset += BATCH_SIZE
        time.sleep(0.5)  # Be gentle to the API

    conn.close()
    print(f"\n Completed! Inserted {total_inserted} records into {DB_FILE}")


if __name__ == "__main__":
    main()
