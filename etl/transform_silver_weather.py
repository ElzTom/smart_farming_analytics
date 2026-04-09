import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_RAIN   = os.path.join(BASE_DIR, "data", "bronze", "weather_rainfall")
BRONZE_TEMP   = os.path.join(BASE_DIR, "data", "bronze", "weather_temperature")
BRONZE_SOLAR  = os.path.join(BASE_DIR, "data", "bronze", "weather_solar")
SILVER_PATH   = os.path.join(BASE_DIR, "data", "silver", "weather_daily")


def get_spark():
    from etl.spark_utils import get_spark as _get_spark
    return _get_spark("Weather-Silver-Transform")


def transform():
    spark = get_spark()

    rain  = spark.read.parquet(BRONZE_RAIN).select("date", col("rainfall_mm"))
    temp  = spark.read.parquet(BRONZE_TEMP).select("date", col("max_temp_c"))
    solar = spark.read.parquet(BRONZE_SOLAR).select("date", col("solar_exposure_mj"))

    # Full outer join on date — keeps all days even if one source has a gap
    df = (
        rain
        .join(temp,  on="date", how="full")
        .join(solar, on="date", how="full")
        .filter(col("date") >= "2023-09-01")
        .withColumn("transformed_at", current_timestamp())
        .orderBy("date")
        .coalesce(1)
    )

    count_out = df.count()
    print(f"[INFO] Writing {count_out} records to silver")

    df.write.mode("overwrite").parquet(SILVER_PATH)

    spark.stop()
    print("✅ Weather silver transform SUCCESS")


if __name__ == "__main__":
    transform()
