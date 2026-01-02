import requests
import sqlite3
from etl.config_loader import load_config


config = load_config()

DB_FILE = config["database"]["db_file"]
SITE_API_URL = config["api"]["site_metadata_url"]
BATCH_SIZE = config["api"]["batch_size"]
MAX_RETRIES = config["api"]["max_retries"]


def fetch_site_metadata():
    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(SITE_API_URL, params={"limit": BATCH_SIZE})
        if response.status_code == 200:
            return response.json().get("results", [])
        retries += 1
    return []


def insert_sites(conn, sites):
    with conn:
        for site in sites:
            conn.execute(
                '''
                INSERT OR IGNORE INTO sites
                (site_id, site_name, property_name, latitude, longitude)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    site.get("site_id"),
                    site.get("site_name"),
                    site.get("property_name"),
                    float(site.get("latitude")),
                    float(site.get("longitude"))
                )
            )


def main():
    conn = sqlite3.connect(DB_FILE)
    sites = fetch_site_metadata()
    insert_sites(conn, sites)
    conn.close()


if __name__ == "__main__":
    main()
