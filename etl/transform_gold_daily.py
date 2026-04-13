import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, avg, when, first
)

from etl.config_loader import load_config

BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_SOIL   = os.path.join(BASE_DIR, "data", "silver", "soil_readings")
SILVER_WEATHER = os.path.join(BASE_DIR, "data", "silver", "weather_daily")
GOLD_PATH     = os.path.join(BASE_DIR, "data", "gold", "daily_site_summary")

config    = load_config()
IRRIGATE_THRESHOLD = config["thresholds"]["soil_moisture_irrigate"]  # 30


def get_spark():
    from etl.spark_utils import get_spark as _get_spark
    return _get_spark("Soil-Gold-Daily")


def pivot_measure(df, measure, value_col_prefix):
    """Pivot a single measure type to wide format: one column per depth per day."""
    return (
        df.filter(col("measure_type") == measure)
        .withColumn("date", to_date(col("event_time")))
        .groupBy("site_name", "site_id", "date")
        .pivot("depth_cm", [10, 20, 30, 40, 50, 60, 70, 80])
        .agg(avg("soil_value"))
        .toDF(
            "site_name", "site_id", "date",
            *[f"{value_col_prefix}_{d}cm" for d in [10, 20, 30, 40, 50, 60, 70, 80]]
        )
    )


def safe_avg(*cols):
    """Average of columns, ignoring NULLs."""
    non_null = [when(col(c).isNotNull(), col(c)) for c in cols]
    count_expr = sum(when(col(c).isNotNull(), 1).otherwise(0) for c in cols)
    sum_expr   = sum(when(col(c).isNotNull(), col(c)).otherwise(0) for c in cols)
    return when(count_expr > 0, sum_expr / count_expr).otherwise(None)


def transform():
    spark = get_spark()

    silver = spark.read.parquet(SILVER_SOIL)

    # ----------------------------------------------------------------
    # 0. Extract soil texture lookup — one texture per site (at 10cm)
    # ----------------------------------------------------------------
    texture_lookup = (
        silver
        .filter(col("depth_cm") == 10)
        .groupBy("site_name", "site_id")
        .agg(first("soil_texture", ignorenulls=True).alias("soil_texture"))
    )

    # ----------------------------------------------------------------
    # 1. Pivot each measure type to wide format
    # ----------------------------------------------------------------
    moisture = pivot_measure(silver, "Moisture",    "moisture")
    temp     = pivot_measure(silver, "Temperature", "temp")
    salinity = pivot_measure(silver, "Salinity",    "salinity")

    # ----------------------------------------------------------------
    # 2. Join all three on site + date
    # ----------------------------------------------------------------
    join_keys = ["site_name", "site_id", "date"]
    df = (
        moisture
        .join(temp,     on=join_keys, how="left")
        .join(salinity, on=join_keys, how="left")
    )

    # ----------------------------------------------------------------
    # 3. Shallow and deep average moisture
    # ----------------------------------------------------------------
    df = df.withColumn(
        "avg_moisture_shallow",
        safe_avg("moisture_10cm", "moisture_20cm", "moisture_30cm", "moisture_40cm")
    ).withColumn(
        "avg_moisture_deep",
        safe_avg("moisture_50cm", "moisture_60cm", "moisture_70cm", "moisture_80cm")
    )

    # ----------------------------------------------------------------
    # 4. Join soil texture lookup
    # ----------------------------------------------------------------
    df = df.join(texture_lookup, on=["site_name", "site_id"], how="left")

    # ----------------------------------------------------------------
    # 5. Join weather on date
    # ----------------------------------------------------------------
    weather = spark.read.parquet(SILVER_WEATHER)
    df = df.join(weather.drop("transformed_at"), on="date", how="left")



    # ----------------------------------------------------------------
    # 6. Irrigation flag
    # ----------------------------------------------------------------
    df = df.withColumn(
        "irrigation_needed",
        (col("avg_moisture_shallow") < IRRIGATE_THRESHOLD) &
        (col("rainfall_mm").isNull() | (col("rainfall_mm") < 2))
    )

    # ----------------------------------------------------------------
    # 7. Metadata
    # ----------------------------------------------------------------
    df = df.withColumn("transformed_at", current_timestamp())

    count_out = df.count()
    print(f"[INFO] Writing {count_out} records to gold")

    df.write.mode("overwrite").parquet(GOLD_PATH)

    spark.stop()
    print("Gold daily transform SUCCESS")


if __name__ == "__main__":
    transform()
