import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp

BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARQUET_FILE  = os.path.join(BASE_DIR, "data", "raw", "soil-sensor-readings-historical-data.parquet")
BRONZE_PATH   = os.path.join(BASE_DIR, "data", "bronze", "soil_readings")

def get_spark():
    from etl.spark_utils import get_spark as _get_spark
    return _get_spark("Soil-Bronze-Parquet-Ingest")

def ingest():
    spark = get_spark()

    # Parquet already has schema embedded — no need to define it manually
    df = spark.read.parquet(PARQUET_FILE)

    df = (
        df
        .withColumnRenamed("local_time",       "Local_Time")
        .withColumnRenamed("site_name",        "Site_Name")
        .withColumnRenamed("site_id",          "Site_ID")
        .withColumnRenamed("id",               "ID")
        .withColumnRenamed("probe_id",         "Probe_ID")
        .withColumnRenamed("probe_measure",    "Probe_Measure")
        .withColumnRenamed("soil_value",       "Soil_Value")
        .withColumnRenamed("unit",             "Unit")
        .withColumn("event_time",    to_timestamp(col("Local_Time")))
        .withColumn("ingested_at",   current_timestamp())
        .withColumn("ingested_date", to_date(col("ingested_at")))
        .filter(col("event_time").isNotNull())
        .coalesce(1)
    )

    print(f"[INFO] Writing {df.count()} records to bronze")

    df.write.mode("overwrite").parquet(BRONZE_PATH)

    spark.stop()
    print("Soil readings bronze ingest SUCCESS")

if __name__ == "__main__":
    ingest()
