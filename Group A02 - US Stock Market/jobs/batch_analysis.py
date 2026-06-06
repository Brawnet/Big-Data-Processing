import sys
import typing
sys.modules['typing.io'] = typing
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, to_date

spark = SparkSession.builder.appName("Stock Batch Analysis").getOrCreate()

# Load Data
df = spark.read.csv("../data/stock_prices_daily.csv", header=True, inferSchema=True)

# 1. Jadikan Date sebagai tipe tanggal murni
df = df.withColumn("Date_Only", to_date("Date"))

# 2. Agregasi Rata-rata per Sektor + Dibuat Harian (Date)
batch_daily_df = df.groupBy("Date_Only", "Sector").agg(
    avg("Close").alias("Avg_Close_Price"),
    sum("Volume").alias("Total_Volume")
).orderBy("Date_Only", "Sector")

# 3. SIMPAN KE HDFS
hdfs_path = "hdfs://localhost:9000/user/data/batch_saham"
local_path = "../data/batch_results/hasil_batch_saham.csv"

try:
    print("Mencoba menyimpan ke HDFS...")
    batch_daily_df.write.mode("overwrite").csv(hdfs_path, header=True)
    print(f"SUKSES: Data Batch di-upload ke HDFS ({hdfs_path})")
except Exception as e:
    print("HDFS tidak terdeteksi. Menyimpan ke sistem lokal sebagai fallback...")

# Bypass toPandas untuk Streamlit
data_koleksi = [row.asDict() for row in batch_daily_df.collect()]
pandas_df = pd.DataFrame(data_koleksi)
os.makedirs("../data/batch_results", exist_ok=True)
pandas_df.to_csv(local_path, index=False)

print("Batch analysis selesai!")
spark.stop()