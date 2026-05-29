import pandas as pd
from kafka import KafkaProducer
import json
import time

# Inisialisasi Kafka Producer dengan tambahan api_version
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 4, 0)  # <--- TAMBAHKAN BARIS INI UNTUK BYPASS ERROR
)

topic_name = 'stock_market'

# Membaca Dataset Saham
print("Membaca dataset saham...")
df = pd.read_csv('../data/stock_prices_daily.csv')
df = df.fillna('') # Handle nilai kosong

print("Mulai mengirim data saham ke Kafka...")
for index, row in df.iterrows():
    record = row.to_dict()
    producer.send(topic_name, record)
    print(f"Terkirim Data Saham: {record['Ticker']} | Harga: {record['Close']}")
    time.sleep(1) # Jeda 1 detik (simulasi streaming)