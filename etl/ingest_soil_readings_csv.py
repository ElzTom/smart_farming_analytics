import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import (
    col, current_timestamp, to_date, to_timestamp
)

# ==========================================================
# PATHS
# ==========================================================
BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_FILE    = os.path.join(BASE_DIR, "data", "raw", "soil-sensor-readings-historical-data.csv")
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "soil_readings")

# Clean output directory
if os.path.exists(BRONZE_PATH):
    import shutil
    shutil.rmtree(BRONZE_PATH)

os.makedirs(BRONZE_PATH, exist_ok=True)

# ==========================================================
# SPARK SESSION (WINDOWS-HARDENED)
# ==========================================================
def get_spark():
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if sys.platform == "win32":
        jdk17 = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
        os.environ["JAVA_HOME"] = jdk17
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = jdk17 + r"\bin;" + r"C:\hadoop\bin;" + os.environ["PATH"]

    spark = (
        SparkSession.builder
        .appName("Soil-Bronze-Ingest")
        .master("local[1]")                 # 🔑 single executor
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
# INGEST
# ==========================================================
def ingest():
    spark = get_spark()

    schema = StructType([
        StructField("Local_Time", StringType(), True),
        StructField("Site_Name", StringType(), True),
        StructField("Site_ID", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("Probe_ID", StringType(), True),
        StructField("Probe_Measure", StringType(), True),
        StructField("Soil_Value", DoubleType(), True),
        StructField("Unit", StringType(), True),
        StructField("json_featuretype", StringType(), True),
    ])

    df = (
        spark.read
        .schema(schema)
        .option("header", True)
        .csv(CSV_FILE)
    )

    df = (
        df
        .withColumn(
            "event_time",
            to_timestamp("Local_Time", "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("ingested_date", to_date(col("ingested_at")))
        .filter(col("event_time").isNotNull())
        .coalesce(1)   # 🔑 single writer
    )

    print(f"[INFO] Writing {df.count()} records")

    (
        df.write
        .mode("overwrite")
        .parquet(BRONZE_PATH)
    )

    spark.stop()
    print("✅ Bronze ingestion SUCCESS")

# ==========================================================
if __name__ == "__main__":
    ingest()
