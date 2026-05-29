# --- 1. PATCH UNTUK PYTHON VERSI BARU ---
import sys
import typing
sys.modules['typing.io'] = typing
# --------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum
import os
import pandas as pd

spark = SparkSession.builder \
    .appName("Stock Batch Analysis") \
    .getOrCreate()

# Load Data CSV Saham
df = spark.read.csv("../data/stock_prices_daily.csv", header=True, inferSchema=True)

# Analisis: Rata-rata Harga & Total Volume berdasar Sector & Industry
analysis_df = df.groupBy("Sector", "Industry") \
    .agg(
        avg("Close").alias("Avg_Close_Price"),
        sum("Volume").alias("Total_Volume")
    ) \
    .orderBy("Sector", "Avg_Close_Price", ascending=False)

analysis_df.show()

# Bypass toPandas
data_koleksi = [row.asDict() for row in analysis_df.collect()]
pandas_df = pd.DataFrame(data_koleksi)

os.makedirs("../data/batch_results", exist_ok=True)
pandas_df.to_csv("../data/batch_results/hasil_batch_saham.csv", index=False)

print("Batch analysis selesai dan tersimpan menggunakan Pandas!")
spark.stop()