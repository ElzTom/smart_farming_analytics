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
    print("❌ Failed to fetch site metadata after max retries.")
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

    print("📥 Fetching site metadata...")
    sites = fetch_site_metadata(SITE_API_URL, limit=BATCH_SIZE)

    if sites:
        print(f"✅ {len(sites)} site metadata records fetched.")
        insert_sites(conn, sites)
        print(f"✅ {len(sites)} site records inserted into database.")
    else:
        print("⚠️ No site metadata found to insert.")

    conn.close()


if __name__ == "__main__":
    main()
