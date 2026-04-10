import os
import time
import requests
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp

from etl.config_loader import load_config

# ==========================================================
# CONFIG
# ==========================================================
config = load_config()

API_URL    = config["api"]["sensor_data_url"]
BATCH_SIZE = config["api"]["batch_size"]
MAX_RETRIES = config["api"]["max_retries"]
TIMEZONE   = config["api"]["timezone"]

# Chunk size in hours — keeps each chunk well under 10k row API limit
CHUNK_HOURS = 12
# Overlap window to catch late-arriving data
SLIDING_WINDOW_MINUTES = 2

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "soil_readings")

SCHEMA = StructType([
    StructField("Local_Time",       StringType(), True),
    StructField("Site_Name",        StringType(), True),
    StructField("Site_ID",          StringType(), True),
    StructField("ID",               StringType(), True),
    StructField("Probe_ID",         StringType(), True),
    StructField("Probe_Measure",    StringType(), True),
    StructField("Soil_Value",       DoubleType(), True),
    StructField("Unit",             StringType(), True),
    StructField("json_featuretype", StringType(), True),
])

# ==========================================================
# SPARK
# ==========================================================
def get_spark():
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("Soil-Bronze-API-Ingest")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ==========================================================
# WATERMARK — read last event_time from bronze Parquet
# ==========================================================
def get_last_timestamp(spark):
    if not os.path.exists(BRONZE_PATH):
        return None
    try:
        df = spark.read.parquet(BRONZE_PATH)
        row = df.agg({"event_time": "max"}).collect()[0][0]
        return row
    except Exception as e:
        print(f"[WARN] Could not read watermark from bronze: {e}")
        return None

# ==========================================================
# TIME CHUNKS — split date range into CHUNK_DAYS windows
# ==========================================================
def build_time_chunks(start_dt, end_dt):
    """Split [start_dt, end_dt] into CHUNK_HOURS-sized windows."""
    chunks = []
    chunk_start = start_dt
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(hours=CHUNK_HOURS), end_dt)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks

# ==========================================================
# API FETCH — fetch all records within a time window
# ==========================================================
def fetch_chunk(from_time, to_time):
    """Fetch all records in [from_time, to_time] using offset pagination.
    Each chunk is small enough to stay under the 10k API limit.
    """
    all_rows = []
    offset = 0

    where_clause = (
        f"local_time >= '{from_time.isoformat()}' "
        f"AND local_time < '{to_time.isoformat()}'"
    )

    while True:
        params = {
            "limit":    BATCH_SIZE,
            "offset":   offset,
            "order_by": "local_time asc",
            "timezone": TIMEZONE,
            "where":    where_clause,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(API_URL, params=params, timeout=30)
                r.raise_for_status()
                batch = r.json().get("results", [])
                break
            except Exception as e:
                print(f"[WARN] API attempt {attempt} failed: {e}")
                time.sleep(2)
                batch = None

        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < BATCH_SIZE:
            break  # last page of this chunk

        offset += len(batch)
        time.sleep(0.3)

    return all_rows

# ==========================================================
# INGEST
# ==========================================================
def ingest():
    spark = get_spark()

    # Determine start point — use watermark with overlap for late data
    last_ts = get_last_timestamp(spark)
    if last_ts:
        # last_ts may be a datetime or pandas Timestamp — normalise to UTC datetime
        if hasattr(last_ts, 'to_pydatetime'):
            last_ts = last_ts.to_pydatetime()
        start_dt = last_ts - timedelta(minutes=SLIDING_WINDOW_MINUTES)
        # Strip timezone if present (API expects naive ISO strings)
        start_dt = start_dt.replace(tzinfo=None)
    else:
        # No bronze data yet — full historical load from API dataset start
        start_dt = datetime(2023, 1, 1)

    end_dt = datetime.now()

    print(f"[INFO] Last bronze timestamp: {last_ts}")
    print(f"[INFO] Fetching from {start_dt} to {end_dt} in {CHUNK_HOURS}-hour chunks")

    chunks = build_time_chunks(start_dt, end_dt)
    print(f"[INFO] Total chunks: {len(chunks)}")

    total_rows = []

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        print(f"[INFO] Chunk {i}/{len(chunks)}: {chunk_start} → {chunk_end}")
        rows = fetch_chunk(chunk_start, chunk_end)
        print(f"[INFO]   → {len(rows)} records")
        total_rows.extend(rows)

    if not total_rows:
        print("[INFO] No new records from API.")
        spark.stop()
        return

    # Convert to Spark DataFrame
    spark_rows = [
        Row(
            Local_Time       = r.get("local_time"),
            Site_Name        = r.get("site_name"),
            Site_ID          = str(r.get("site_id"))   if r.get("site_id")   is not None else None,
            ID               = str(r.get("id"))         if r.get("id")         is not None else None,
            Probe_ID         = str(r.get("probe_id"))  if r.get("probe_id")  is not None else None,
            Probe_Measure    = r.get("probe_measure"),
            Soil_Value       = float(r["soil_value"])  if r.get("soil_value") is not None else None,
            Unit             = r.get("unit"),
            json_featuretype = r.get("json_featuretype"),
        )
        for r in total_rows
    ]

    df = spark.createDataFrame(spark_rows, schema=SCHEMA)

    df = (
        df
        .withColumn("event_time",    to_timestamp("Local_Time", "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("ingested_at",   current_timestamp())
        .withColumn("ingested_date", to_date(col("ingested_at")))
        .filter(col("event_time").isNotNull())
        .coalesce(1)
    )

    count = df.count()
    print(f"[INFO] Writing {count} records to bronze (append)")

    (
        df.write
        .mode("append")
        .parquet(BRONZE_PATH)
    )

    spark.stop()
    print("✅ Bronze API ingest SUCCESS")

# ==========================================================
if __name__ == "__main__":
    ingest()
