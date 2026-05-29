# --- 1. PATCH UNTUK PYTHON VERSI BARU ---
import sys
import typing
sys.modules['typing.io'] = typing

# --- 2. PATCH HADOOP / WINUTILS OTOMATIS ---
import os
import urllib.request

hadoop_dir = "C:\\hadoop\\bin"
os.makedirs(hadoop_dir, exist_ok=True)
winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe"
hadoop_dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll"

if not os.path.exists(os.path.join(hadoop_dir, "winutils.exe")):
    urllib.request.urlretrieve(winutils_url, os.path.join(hadoop_dir, "winutils.exe"))
if not os.path.exists(os.path.join(hadoop_dir, "hadoop.dll")):
    urllib.request.urlretrieve(hadoop_dll_url, os.path.join(hadoop_dir, "hadoop.dll"))

os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] = hadoop_dir + os.pathsep + os.environ.get('PATH', '')
# ---------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("Stock Streaming Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema data saham
schema = StructType() \
    .add("Ticker", StringType()) \
    .add("Sector", StringType()) \
    .add("Close", DoubleType()) \
    .add("Volume", LongType())

# Membaca dari Kafka (Topik baru: stock_market)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_market") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agregasi: Rata-rata Harga per Sektor secara Real-time
agg_df = parsed_df.groupBy("Sector").mean("Close")

query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()