import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 4, 0)
)

print("Membaca dan memproses dataset...")
df = pd.read_csv('../data/stock_prices_daily.csv')

# 1. Parsing & Urutkan per Timestamp
df['Date'] = pd.to_datetime(df['Date'], utc=True)
df = df.sort_values(by='Date')

# 2. Filter Data Stream hanya 2024 - 2026
df_stream = df[(df['Date'].dt.year >= 2024) & (df['Date'].dt.year <= 2026)].copy()
df_stream['Date'] = df_stream['Date'].dt.strftime('%Y-%m-%d') # Format ulang ke string
df_stream = df_stream.fillna('')

print("Mulai mengirim data saham (2024-2026) ke Kafka (Kecepatan: 200 data/detik)...")

jumlah_terkirim = 0

for index, row in df_stream.iterrows():
    record = row.to_dict()
    producer.send('stock_market', record)
    
    jumlah_terkirim += 1
    
    # Cek apakah sudah 200 data yang dikirim
    if jumlah_terkirim % 200 == 0:
        print(f"[{record['Date']}] Sukses mengirim {jumlah_terkirim} baris data... (jeda 1 detik)")
        time.sleep(1) # Istirahat 1 detik setiap kelipatan 200