import os
import pandas as pd
from sqlalchemy import create_engine, text

BASE_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DAILY_PATH   = os.path.join(BASE_DIR, "data", "gold", "daily_site_summary")
HOURLY_PATH  = os.path.join(BASE_DIR, "data", "gold", "hourly_site_summary")

DB_URL = "postgresql+psycopg2://farming:farming123@localhost:5432/smart_farming"


def load_table(engine, parquet_path, table_name):
    print(f"[INFO] Loading {table_name}...")
    df = pd.read_parquet(parquet_path)

    # Strip timezone from timestamp columns — Postgres handles tz-naive cleaner
    for c in df.select_dtypes(include=["datetimetz"]).columns:
        df[c] = df[c].dt.tz_localize(None)

    df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=5000)
    print(f"[INFO] {table_name}: {len(df)} rows loaded ✓")


def load():
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[INFO] Connected to PostgreSQL ✓")

    load_table(engine, DAILY_PATH,  "daily_site_summary")
    load_table(engine, HOURLY_PATH, "hourly_site_summary")

    print("✅ Gold tables loaded to PostgreSQL")


if __name__ == "__main__":
    load()
