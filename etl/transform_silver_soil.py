import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg, count,
    regexp_extract, when
)

from etl.config_loader import load_config

BASE_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_PATH  = os.path.join(BASE_DIR, "data", "bronze", "soil_readings")
SILVER_PATH  = os.path.join(BASE_DIR, "data", "silver", "soil_readings")

config     = load_config()
thresholds = config["thresholds"]


def get_spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName("Soil-Silver-Transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def transform():
    spark = get_spark()

    df = spark.read.parquet(BRONZE_PATH)

    # ----------------------------------------------------------------
    # 1. Extract depth_cm  (handles "40 cm" with optional space)
    # ----------------------------------------------------------------
    depth_str = regexp_extract(col("Probe_Measure"), r"(\d+)\s*[Cc][Mm]", 1)
    df = df.withColumn(
        "depth_cm",
        when(depth_str != "", depth_str.cast("int")).otherwise(None)
    )

    # ----------------------------------------------------------------
    # 2. Extract measure_type
    # ----------------------------------------------------------------
    df = df.withColumn(
        "measure_type",
        when(col("Probe_Measure").contains("Moisture"),    "Moisture")
        .when(col("Probe_Measure").contains("Temperature"), "Temperature")
        .when(col("Probe_Measure").contains("Salinity"),   "Salinity")
        .otherwise(None)
    )

    # ----------------------------------------------------------------
    # 3. Extract soil_texture  (Clay, Sandy Loam, Clayey Loam, Loam)
    # ----------------------------------------------------------------
    df = df.withColumn(
        "soil_texture",
        regexp_extract(col("Probe_Measure"), r"\((Clay[^)]*|Sandy[^)]*|Loam[^)]*)\)", 1)
    )
    # Replace empty string (no match) with null
    df = df.withColumn(
        "soil_texture",
        when(col("soil_texture") == "", None).otherwise(col("soil_texture"))
    )

    # ----------------------------------------------------------------
    # 4. Quality filters — use thresholds from config.json
    # ----------------------------------------------------------------
    moisture_min    = thresholds["moisture_min"]
    moisture_max    = thresholds["moisture_max"]
    temp_min        = thresholds["temperature_min"]
    temp_max        = thresholds["temperature_max"]
    salinity_min    = thresholds["salinity_min"]
    salinity_max    = thresholds["salinity_max"]

    df = df.filter(
        col("measure_type").isNotNull() &
        col("depth_cm").isNotNull() &
        col("event_time").isNotNull() &
        (
            ((col("measure_type") == "Moisture")    & col("Soil_Value").between(moisture_min,  moisture_max))  |
            ((col("measure_type") == "Temperature") & col("Soil_Value").between(temp_min,      temp_max))      |
            ((col("measure_type") == "Salinity")    & col("Soil_Value").between(salinity_min,  salinity_max))
        )
    )

    # ----------------------------------------------------------------
    # 5. Deduplicate — average across multiple Probe_IDs at same
    #    site + depth + measure + timestamp
    # ----------------------------------------------------------------
    df = (
        df.groupBy("Site_Name", "Site_ID", "event_time", "depth_cm", "measure_type", "soil_texture")
        .agg(
            avg("Soil_Value").alias("soil_value"),
            count("Soil_Value").alias("reading_count"),
        )
    )

    # ----------------------------------------------------------------
    # 6. Rename to snake_case and add metadata
    # ----------------------------------------------------------------
    df = (
        df
        .withColumnRenamed("Site_Name", "site_name")
        .withColumnRenamed("Site_ID",   "site_id")
        .withColumn("transformed_at", current_timestamp())
    )

    count_out = df.count()
    print(f"[INFO] Writing {count_out} records to silver")

    (
        df.write
        .mode("overwrite")
        .partitionBy("measure_type")
        .parquet(SILVER_PATH)
    )

    spark.stop()
    print("✅ Soil silver transform SUCCESS")


if __name__ == "__main__":
    transform()
