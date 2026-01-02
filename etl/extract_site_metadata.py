import requests
import sqlite3
from etl.config_loader import load_config

# Load config
config = load_config()

DB_FILE = config["database"]["db_file"]
SITE_API_URL = config["api"]["site_metadata_url"]
BATCH_SIZE = config["api"]["batch_size"]
MAX_RETRIES = config["api"]["max_retries"]


def fetch_site_metadata(api_url, limit=100):
    params = {"limit": limit}
    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            return response.json().get("results", [])
        else:
            print(f"Request failed ({response.status_code}). Retrying ({retries + 1})...")
            retries += 1
    print(" Failed to fetch site metadata after max retries.")
    return []


def insert_sites(conn, sites):
    with conn:
        for site in sites:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO sites
                    (site_id, site_name, property_name, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    site.get("site_id"),
                    site.get("site_name"),
                    site.get("property_name"),
                    float(site.get("latitude")),
                    float(site.get("longitude"))
                ))
            except Exception as e:
                print(f"Error inserting site {site.get('site_id')}: {e}")


def main():
    conn = sqlite3.connect(DB_FILE)

    print(" Fetching site metadata...")
    sites = fetch_site_metadata(SITE_API_URL, limit=BATCH_SIZE)

    if sites:
        print(f" {len(sites)} site metadata records fetched.")
        insert_sites(conn, sites)
        print(f" {len(sites)} site records inserted into database.")
    else:
        print(" No site metadata found to insert.")

    conn.close()


if __name__ == "__main__":
    main()


""""""
# import requests
import sqlite3
import time

API_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/soil-sensor-readings-historical-data/records"
BATCH_SIZE = 100
MAX_RETRIES = 3
MAX_RECORDS = 1000000000  # max records to fetch

DB_FILE = "data/soil_sensor_data.db"

def fetch_data(limit, last_timestamp=None):
    params = {
        "limit": limit,
        "order_by": "local_time asc",
        "timezone": "Australia/Sydney"
    }
    if last_timestamp:
        params["where"] = f"local_time > '{last_timestamp}'"

    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(API_URL, params=params)
        if response.status_code == 200:
            return response.json()["results"]
        else:
            print(f"Request failed. Retrying ({retries + 1})...")
            retries += 1
            time.sleep(2)
    print(" Failed after max retries.")
    return []

def create_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_soil_data (
            id TEXT PRIMARY KEY,
            local_time TEXT,
            site_name TEXT,
            site_id TEXT,
            probe_id TEXT,
            probe_measure TEXT,
            soil_value REAL,
            unit TEXT,
            json_featuretype TEXT
        )
    ''')
    conn.commit()

def insert_records(conn, records):
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
    create_table(conn)

    all_records_count = 0
    last_timestamp = None

    while all_records_count < MAX_RECORDS:
        batch = fetch_data(BATCH_SIZE, last_timestamp)
        if not batch:
            break

        insert_records(conn, batch)
        all_records_count += len(batch)
        print(f" Inserted {all_records_count} records so far...")

        last_timestamp = batch[-1]["local_time"]

        time.sleep(0.5)

        if all_records_count >= MAX_RECORDS:
            break

    conn.close()
    print(f"\n Completed! Inserted {all_records_count} records into {DB_FILE}")

if __name__ == "__main__":
    main()

""""""