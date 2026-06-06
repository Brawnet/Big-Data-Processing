import sys
import typing
sys.modules['typing.io'] = typing
import os
import urllib.request

# Patch Hadoop untuk Windows
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("Stock Streaming Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema lengkap untuk grafik Candlestick
schema = StructType() \
    .add("Date", StringType()) \
    .add("Ticker", StringType()) \
    .add("Open", DoubleType()) \
    .add("High", DoubleType()) \
    .add("Low", DoubleType()) \
    .add("Close", DoubleType()) \
    .add("Volume", LongType())

# Membaca dari Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_market") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# MENYIMPAN DATA STREAMING KE FOLDER (Agar bisa dibaca Streamlit)
stream_path = "../data/stream_output"
checkpoint_path = "../data/stream_checkpoint"

print(f"Menyimpan data real-time ke {stream_path} ...")
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", stream_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()