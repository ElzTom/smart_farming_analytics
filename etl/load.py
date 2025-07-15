import sqlite3
from etl.config_loader import load_config


config = load_config()
DB_FILE = config["database"]["db_file"]

def create_tables():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create raw sensor data table
    cursor.execute('''
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

    # Create site metadata table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            site_id INTEGER PRIMARY KEY,
            site_name TEXT,
            property_name TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')

    conn.commit()
    conn.close()
    print(f" Tables created in {DB_FILE}")


if __name__ == "__main__":
    create_tables()
