import sqlite3
import csv
import os

DB_FILE = "data/weather_data.db"
CSV_FILE = r"C:\Users\eliza\OneDrive\Desktop\Smart Farming Datasets\Weather dataset\Rainfall\IDCJAC0009_086338_1800_Data.csv"
def sanitize(value):
    if value is None or value == "":
        return None
    return value.strip() if isinstance(value, str) else value

def create_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS rainfall (
            station_number TEXT,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            rainfall_mm REAL,
            period_days REAL,
            quality TEXT,
            PRIMARY KEY (station_number, year, month, day)
        )
    ''')

def ingest_csv(conn, csv_file):
    with open(csv_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        inserted = 0
        for row in reader:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO rainfall
                    (station_number, year, month, day, rainfall_mm, period_days, quality)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sanitize(row.get("Bureau of Meteorology station number")),
                    int(row.get("Year")),
                    int(row.get("Month")),
                    int(row.get("Day")),
                    float(row.get("Rainfall amount (millimetres)")) if row.get("Rainfall amount (millimetres)") else None,
                    float(row.get("Period over which rainfall was measured (days)")) if row.get("Period over which rainfall was measured (days)") else None,
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
    print("Rainfall ingestion complete!")

if __name__ == "__main__":
    main()
