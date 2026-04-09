import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg, when, first, date_trunc
)

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_SOIL = os.path.join(BASE_DIR, "data", "silver", "soil_readings")
GOLD_PATH   = os.path.join(BASE_DIR, "data", "gold", "hourly_site_summary")


def get_spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName("Soil-Gold-Hourly")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def pivot_measure(df, measure, value_col_prefix):
    """Pivot a single measure type to wide format: one column per depth per hour."""
    return (
        df.filter(col("measure_type") == measure)
        .withColumn("hour", date_trunc("hour", col("event_time")))
        .groupBy("site_name", "site_id", "hour")
        .pivot("depth_cm", [10, 20, 30, 40, 50, 60, 70, 80])
        .agg(avg("soil_value"))
        .toDF(
            "site_name", "site_id", "hour",
            *[f"{value_col_prefix}_{d}cm" for d in [10, 20, 30, 40, 50, 60, 70, 80]]
        )
    )


def safe_avg(*cols):
    """Average of columns, ignoring NULLs."""
    count_expr = sum(when(col(c).isNotNull(), 1).otherwise(0) for c in cols)
    sum_expr   = sum(when(col(c).isNotNull(), col(c)).otherwise(0) for c in cols)
    return when(count_expr > 0, sum_expr / count_expr).otherwise(None)


def transform():
    spark = get_spark()

    silver = spark.read.parquet(SILVER_SOIL)

    # ----------------------------------------------------------------
    # 0. Soil texture lookup — one texture per site (at 10cm)
    # ----------------------------------------------------------------
    texture_lookup = (
        silver
        .filter(col("depth_cm") == 10)
        .groupBy("site_name", "site_id")
        .agg(first("soil_texture", ignorenulls=True).alias("soil_texture"))
    )

    # ----------------------------------------------------------------
    # 1. Pivot each measure type to wide format (hourly grain)
    # ----------------------------------------------------------------
    moisture = pivot_measure(silver, "Moisture",    "moisture")
    temp     = pivot_measure(silver, "Temperature", "temp")
    salinity = pivot_measure(silver, "Salinity",    "salinity")

    # ----------------------------------------------------------------
    # 2. Join all three on site + hour
    # ----------------------------------------------------------------
    join_keys = ["site_name", "site_id", "hour"]
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
    # 4. Join soil texture
    # ----------------------------------------------------------------
    df = df.join(texture_lookup, on=["site_name", "site_id"], how="left")

    # ----------------------------------------------------------------
    # 5. Metadata
    # ----------------------------------------------------------------
    df = df.withColumn("transformed_at", current_timestamp())

    count_out = df.count()
    print(f"[INFO] Writing {count_out} records to gold hourly")

    df.write.mode("overwrite").parquet(GOLD_PATH)

    spark.stop()
    print("✅ Gold hourly transform SUCCESS")


if __name__ == "__main__":
    transform()
