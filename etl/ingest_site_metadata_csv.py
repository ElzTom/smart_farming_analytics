import sqlite3
import csv
import os

DB_FILE = "data/soil_sensor_data.db"
CSV_FILE = r"C:\Users\eliza\Downloads\soil-sensor-locations.csv"

def sanitize(value):
    """Clean string values for SQLite."""
    if value is None:
        return ""
    return str(value).strip()

def create_table(conn):
    """Create sites table if it doesn't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            site_id INTEGER PRIMARY KEY,
            site_name TEXT,
            property_name TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')

def ingest_csv(conn, csv_file):
    """Read site metadata CSV and insert into database."""
    with open(csv_file, newline='', encoding='utf-8-sig') as f:  # <-- utf-8-sig fixes BOM
        reader = csv.DictReader(f)
        inserted = 0

        for row in reader:
            # Strip spaces from keys and values
            row = {k.strip(): (v.strip() if v else None) for k, v in row.items()}

            site_id_raw = row.get("Site_ID")
            if not site_id_raw:
                print(f"Skipping row, missing Site_ID: {row}")
                continue

            try:
                site_id = int(site_id_raw)
                site_name = sanitize(row.get("Site_Name"))
                property_name = sanitize(row.get("Property_Name"))
                latitude = float(row.get("Latitude")) if row.get("Latitude") else None
                longitude = float(row.get("Longitude")) if row.get("Longitude") else None

                conn.execute('''
                    INSERT OR IGNORE INTO sites
                    (site_id, site_name, property_name, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?)
                ''', (site_id, site_name, property_name, latitude, longitude))
                inserted += 1
            except Exception as e:
                print(f"Error inserting site {site_id_raw}: {e}")

        print(f"\nInserted {inserted} rows from CSV into database.")

def main():
    # Ensure 'data' folder exists
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    ingest_csv(conn, CSV_FILE)
    conn.commit()
    conn.close()
    print("Site metadata CSV ingestion complete!")

if __name__ == "__main__":
    main()
