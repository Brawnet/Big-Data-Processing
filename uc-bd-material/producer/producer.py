import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time

# ==========================================
# 1. PERSIAPAN TOPIK (Mencegah Leader Not Available)
# ==========================================
print("Mengecek dan menyiapkan ruangan Kafka...")
try:
    # Ubah ini:
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    # Membuat topik secara resmi dengan 1 partisi dan 1 replika
    topic_baru = NewTopic(name="stock_market", num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic_baru])
    print("✅ Ruangan Topik 'stock_market' berhasil dibangun!")
except TopicAlreadyExistsError:
    print("✅ Ruangan Topik 'stock_market' sudah siap digunakan.")
except Exception as e:
    print(f"⚠️ Peringatan Admin: {e}")

# ==========================================
# 2. PRODUCER MENGIRIM DATA
# ==========================================
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # Hapus baris api_version=(3,4,0) karena kadang membuat error di Windows
)

print("Membaca dan memproses dataset...")
df = pd.read_csv('../data/stock_prices_daily.csv')

df['Date'] = pd.to_datetime(df['Date'], utc=True)
df = df.sort_values(by='Date')

df_stream = df[(df['Date'].dt.year >= 2024) & (df['Date'].dt.year <= 2026)].copy()
df_stream['Date'] = df_stream['Date'].dt.strftime('%Y-%m-%d')
df_stream = df_stream.fillna('')

print("Mulai mengirim data saham (2024-2026) ke Kafka (Kecepatan: 200 data/detik)...")
jumlah_terkirim = 0

for index, row in df_stream.iterrows():
    record = row.to_dict()
    producer.send('stock_market', record)
    
    jumlah_terkirim += 1
    
    if jumlah_terkirim % 100 == 0:
        print(f"[{record['Date']}] Sukses mengirim {jumlah_terkirim} baris data... (jeda 1 detik)")
        time.sleep(1)

# ... (Bagian atas persiapan admin Kafka biarkan sama) ...

print("Mulai mengirim data saham (2024-2026) ke Kafka (Kecepatan: 100 data/detik)...")

# --- TAMBAHKAN WHILE TRUE DI SINI AGAR DATANYA MENGALIR TERUS ---
while True:
    jumlah_terkirim = 0
    
    for index, row in df_stream.iterrows():
        record = row.to_dict()
        producer.send('stock_market', record)
        
        jumlah_terkirim += 1
        
        if jumlah_terkirim % 100 == 0:
            print(f"[{record['Date']}] Sukses mengirim {jumlah_terkirim} baris data... (jeda 1 detik)")
            time.sleep(1)
            
    print("🔄 Dataset habis, memutar ulang data dari awal untuk simulasi streaming...")