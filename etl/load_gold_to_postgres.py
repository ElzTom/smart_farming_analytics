import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DAILY_PATH   = os.path.join(BASE_DIR, "data", "gold", "daily_site_summary")
HOURLY_PATH  = os.path.join(BASE_DIR, "data", "gold", "hourly_site_summary")

DB_URL = os.environ.get(
    "SMART_FARMING_DB_URL",
    "postgresql+psycopg2://farming:farming123@localhost:5432/smart_farming"
)


def url_to_dsn(url):
    url = url.replace("postgresql+psycopg2://", "")
    creds, rest = url.split("@")
    user, password = creds.split(":")
    hostport, dbname = rest.split("/")
    host, port = hostport.split(":")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def load_table(dsn, parquet_path, table_name):
    print(f"[INFO] Loading {table_name}...")
    df = pd.read_parquet(parquet_path)

    # Strip timezone from timestamp columns
    for c in df.select_dtypes(include=["datetimetz"]).columns:
        df[c] = df[c].dt.tz_localize(None)

    # Replace NaN/NaT with None for psycopg2
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    col_str = ", ".join(f'"{c}"' for c in cols)

    type_map = {
        "int64": "BIGINT", "int32": "INTEGER", "float64": "DOUBLE PRECISION",
        "float32": "REAL", "bool": "BOOLEAN", "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP", "date": "DATE"
    }

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    col_defs = []
    for c, dtype in df.dtypes.items():
        pg_type = type_map.get(str(dtype), "TEXT")
        col_defs.append(f'"{c}" {pg_type}')
    cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    execute_values(cur, f'INSERT INTO "{table_name}" ({col_str}) VALUES %s', rows, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[INFO] {table_name}: {len(df)} rows loaded ✓")


def load():
    dsn = url_to_dsn(DB_URL)
    conn = psycopg2.connect(dsn)
    conn.close()
    print("[INFO] Connected to PostgreSQL ✓")

    load_table(dsn, DAILY_PATH,  "daily_site_summary")
    load_table(dsn, HOURLY_PATH, "hourly_site_summary")

    print("✅ Gold tables loaded to PostgreSQL")


if __name__ == "__main__":
    load()
