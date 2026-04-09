import os
import sys


def get_spark(app_name="SmartFarming"):
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if sys.platform == "win32":
        jdk17 = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
        os.environ["JAVA_HOME"] = jdk17
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = jdk17 + r"\bin;" + r"C:\hadoop\bin;" + os.environ["PATH"]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        # Disable Hadoop chmod — needed when writing to NTFS/Windows mounts
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
