import sqlite3
import csv
import os

# SQLite DB file (same as rainfall, so all weather data in one DB)
DB_FILE = "data/weather_data.db"

# CSV file path for temperature
CSV_FILE = r"C:\Users\eliza\OneDrive\Desktop\Smart Farming Datasets\Weather dataset\Temp\IDCJAC0010_086338_1800_Data.csv"

def sanitize(value):
    """Clean data to store in SQLite."""
    if value is None or value == "":
        return None
    return value.strip() if isinstance(value, str) else value

def create_table(conn):
    """Create temperature table if it doesn't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS temperature (
            station_number TEXT,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            max_temp REAL,
            accumulation_days REAL,
            quality TEXT,
            PRIMARY KEY (station_number, year, month, day)
        )
    ''')

def ingest_csv(conn, csv_file):
    """Read CSV and insert into SQLite."""
    with open(csv_file, newline='', encoding='utf-8-sig') as f:  # remove BOM
        reader = csv.DictReader(f)
        inserted = 0
        for row in reader:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO temperature
                    (station_number, year, month, day, max_temp, accumulation_days, quality)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sanitize(row.get("Bureau of Meteorology station number")),
                    int(row.get("Year")),
                    int(row.get("Month")),
                    int(row.get("Day")),
                    float(row.get("Maximum temperature (Degree C)")) if row.get("Maximum temperature (Degree C)") else None,
                    float(row.get("Days of accumulation of maximum temperature")) if row.get("Days of accumulation of maximum temperature") else None,
                    sanitize(row.get("Quality"))
                ))
                inserted += 1
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        print(f"\nInserted {inserted} rows from CSV.")

def main():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

    conn = sqlite3.connect(DB_FILE)
    create_table(conn)
    ingest_csv(conn, CSV_FILE)
    conn.commit()
    conn.close()
    print("Temperature ingestion complete!")

if __name__ == "__main__":
    main()
