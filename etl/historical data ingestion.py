import sqlite3
import csv
from datetime import datetime
import os  # <-- added

DB_FILE = "data/soil_sensor_data.db"
CSV_FILE = r"C:\Users\eliza\Downloads\soil-sensor-readings-historical-data (1).csv"  # Updated path

def sanitize(value):
    """Clean data for SQLite."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.replace("#", " ")  # remove # to avoid SQLite issues
    return value

def create_table(conn):
    """Create the table if it doesn't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_soil_data (
            Local_Time TEXT,
            Site_Name TEXT,
            Site_ID INTEGER,
            ID INTEGER PRIMARY KEY,
            Probe_ID INTEGER,
            Probe_Measure TEXT,
            Soil_Value REAL,
            Unit TEXT,
            json_featuretype TEXT
        )
    ''')

def ingest_csv(conn, csv_file):
    """Read CSV and insert into SQLite."""
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        inserted = 0
        for row in reader:
            try:
                # Handle empty Soil_Value
                soil_value_str = row.get("Soil_Value")
                soil_value = float(soil_value_str) if soil_value_str else None  # or 0.0

                conn.execute('''
                    INSERT OR IGNORE INTO raw_soil_data
                    (Local_Time, Site_Name, Site_ID, ID, Probe_ID, Probe_Measure, Soil_Value, Unit, json_featuretype)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sanitize(row.get("Local_Time")),
                    sanitize(row.get("Site_Name")),
                    int(row.get("Site_ID")),
                    int(row.get("ID")),
                    int(row.get("Probe_ID")),
                    sanitize(row.get("Probe_Measure")),
                    soil_value,
                    sanitize(row.get("Unit")),
                    sanitize(row.get("json_featuretype"))
                ))
                inserted += 1
            except Exception as e:
                print(f"Error inserting row {row.get('ID')}: {e}")

        print(f"\nInserted {inserted} rows from CSV into database.")


def main():
    # Ensure 'data' folder exists
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    ingest_csv(conn, CSV_FILE)
    conn.commit()
    conn.close()
    print("CSV ingestion complete!")

if __name__ == "__main__":
    main()
