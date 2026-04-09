import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, to_date, lpad, concat_ws

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_FILE    = os.path.join(BASE_DIR, "data", "raw", "weather", "rainfall", "IDCJAC0009_086338_1800_Data.csv")
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "weather_rainfall")

SCHEMA = StructType([
    StructField("product_code",   StringType(),  True),
    StructField("station_number", StringType(),  True),
    StructField("year",           IntegerType(), True),
    StructField("month",          IntegerType(), True),
    StructField("day",            IntegerType(), True),
    StructField("rainfall_mm",    DoubleType(),  True),
    StructField("period_days",    DoubleType(),  True),
    StructField("quality",        StringType(),  True),
])

def get_spark():
    from etl.spark_utils import get_spark as _get_spark
    return _get_spark("Weather-Rainfall-Bronze-Ingest")

def ingest():
    spark = get_spark()

    df = (
        spark.read
        .schema(SCHEMA)
        .option("header", True)
        .csv(CSV_FILE)
    )

    df = (
        df
        .withColumn(
            "date",
            to_date(
                concat_ws("-", col("year"), lpad(col("month"), 2, "0"), lpad(col("day"), 2, "0")),
                "yyyy-MM-dd"
            )
        )
        .withColumn("ingested_at", current_timestamp())
        .filter(col("date").isNotNull())
        .coalesce(1)
    )

    print(f"[INFO] Writing {df.count()} records to bronze")

    df.write.mode("overwrite").parquet(BRONZE_PATH)

    spark.stop()
    print("✅ Weather rainfall bronze ingest SUCCESS")

if __name__ == "__main__":
    ingest()
