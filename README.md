# 📈 Big Data Processing — Stock Market Pipeline

## Group A02 Members:

| Nama | NIM |
|---|---|
| Brant Marvel Santosa | 0706022310005 |
| M. Ilham Fadhilah Wirayudha | 0706022310062 |
| Moch. Royhan Firdaus A | 0706022310057 |
| Muh. Nur Alif Akbar | 0706022310031 |
| Yehezkiel Chandra | 0706022310038 |


## Project Demonstration https://youtu.be/qx72UBF5SpQ (play it 2x)
---

## Project Description

**Domain Pilihan**

Proyek ini memilih domain **Pasar Keuangan (Financial Market)**, dengan fokus khusus pada analisis pergerakan harga saham dan volume perdagangan harian di bursa saham Amerika Serikat (US Stock Market). Domain ini dipilih karena memiliki karakteristik high-velocity data (data dengan kecepatan tinggi) dan struktur temporal yang sangat kuat, menjadikannya studi kasus yang sangat relevan untuk menguji keandalan infrastruktur Big Data dalam menangani data historis sekaligus aliran data real-time.

**Sistem yang Dibangun**

Sistem yang dibangun adalah sebuah **Pipeline Data End-to-End hibrida (Lambda Architecture-inspired)**. Sistem ini mengintegrasikan beberapa teknologi Big Data utama untuk memisahkan beban kerja analisis:
- **Ingestion Layer:** Menggunakan **Apache Kafka** (Zookeeper & Kafka Broker via Docker) sebagai message broker terdistribusi untuk menerima dan mengantrekan aliran data pasar saham.
- **Processing Layer:** Menggunakan **Apache Spark (PySpark)** yang dibagi menjadi dua mesin pemrosesan terpisah: **PySpark Core** untuk kebutuhan Batch Processing (analisis data historis skala besar) dan **PySpark Structured** Streaming untuk kebutuhan Stream Processing (analisis data real-time dari Kafka).
- **Presentation Layer:** Menggunakan Streamlit Dashboard interaktif yang didukung oleh pustaka Plotly untuk memvisualisasikan data gabungan dari hasil pemrosesan batch dan streaming secara dinamis.

**Alur Kerja Sistem (End-to-End)**

Secara end-to-end, sistem ini bekerja melalui mekanisme interaksi komponen berikut:

##### **1. Simulasi & Ingesti Data (Producer):**

Skrip Kafka Producer (`producer.py`) membaca dataset mentah secara sekuensial dan menyimulasikan aktivitas pasar saham live. Data saham untuk rentang tahun 2024–2026 dikirimkan secara kontinu dengan kecepatan tinggi (100 data/detik) ke dalam Kafka Topic bernama `stock_market`.
Jalur Pemrosesan Batch (Batch Processing Pipeline):
Secara berkala atau satu waktu, skrip batch_analysis.py mengeksekusi pemrosesan menggunakan Apache Spark untuk membaca data historis (tahun 2020–2023) langsung dari repositori data. Spark melakukan transformasi data, menyamakan tipe data temporal, dan melakukan agregasi berat (menghitung rata-rata harga penutupan dan total volume perdagangan harian yang dikelompokkan per sektor industri). Hasil agregasi batch ini disimpan ke dalam HDFS (Hadoop Distributed File System) dengan fallback otomatis ke berkas CSV lokal (`hasil_batch_saham.csv`) agar siap dikonsumsi dashboard.

##### **2. Jalur Pemrosesan Streaming (Real-Time Pipeline):**

Secara paralel, skrip `streaming_job.py` yang berbasis PySpark Structured Streaming terus memantau (subscribe) Kafka Topic `stock_market`. Setiap ada tick data baru yang masuk dari producer, Spark langsung menangkapnya secara real-time, memvalidasi struktur datanya sesuai skema Candlestick, dan menuliskan hasilnya ke dalam direktori output streaming dalam bentuk pecahan berkas JSON mikro (`../data/stream_output/*.json`).

##### **3. Konsumsi & Visualisasi Dashboard (Streamlit):**

Aplikasi dashboard Streamlit (`app.py`) bertindak sebagai muara akhir. Saat dijalankan, aplikasi ini membaca data batch historis untuk menampilkan tren makro sektor industri. Ketika fitur Auto-Refresh diaktifkan, Streamlit akan melakukan pemindaian berkala setiap 3 detik ke folder output streaming. Aplikasi ini secara cerdas menggabungkan (concatenate) data historis sebelum 2024 dengan data live baru yang masuk dari Spark, lalu merendernya ke dalam grafik Candlestick dinamis dan Line Chart Plotly secara real-time.

---

## Problem Statement

Pipeline ini dirancang untuk menjawab dua pertanyaan bisnis utama:

**Batch Insight:**
> *Sektor industri mana yang secara konsisten memiliki rata-rata harga penutupan tertinggi dan volume perdagangan terbesar sepanjang periode 2020–2023?*

Pertanyaan ini dijawab oleh PySpark batch job (`batch_analysis.py`) yang mengagregasi seluruh dataset historis — menghitung rata-rata harga penutupan (`Avg_Close_Price`) dan total volume (`Total_Volume`) per sektor per hari. Hasilnya tersimpan di `hasil_batch_saham.csv` dan divisualisasikan di dashboard.

**Real-Time Metric:**
> *Berapa banyak data tick saham yang masuk per detik secara real-time, dan saham mana yang paling aktif diperdagangkan saat ini?*

Pertanyaan ini dijawab oleh PySpark Structured Streaming (`streaming_job.py`) yang mengkonsumsi data dari Kafka topic `stock_market` secara kontinu. Dashboard menampilkan data streaming terbaru dengan auto-refresh setiap 3 detik, memungkinkan pemantauan aktivitas perdagangan secara langsung.

---

## Struktur Proyek

```
Big-Data-Processing-main/
└── uc-bd-material/
    ├── dashboard/
    │   ├── app.py              # Streamlit dashboard (visualisasi batch + streaming)
    │   ├── dockerfile          # Docker image untuk dashboard
    │   └── requirements.txt    # Dependensi dashboard (streamlit, pandas, plotly)
    ├── data/
    │   ├── stock_prices_daily.csv          # Dataset utama harga saham harian
    │   └── batch_results/
    │       └── hasil_batch_saham.csv       # Output hasil batch analysis (PySpark)
    |       └── hasil_stream_analisis       # output kalau mengeluarkan 
    ├── jobs/
    │   ├── batch_analysis.py       # PySpark job untuk batch processing
    │   └── streaming_job.py        # PySpark Structured Streaming dari Kafka
    ├── producer/
    │   ├── producer.py             # Kafka Producer (mensimulasikan data real-time)
    │   └── requirements.txt        # Dependensi producer (kafka-python, pandas)
    └── docker-compose.yml          # Orkestrasi Zookeeper + Kafka
```

---

## Arsitektur Sistem

```
JALUR BATCH
                            ┌───────────────────────┐
                            │      HDFS Cluster     │
                            │ (stock_prices_        │
                            │    daily.csv)         │
                            └───────────┬───────────┘
                                        │
                                        │ (Read Batch)
                                        ▼
                            ┌───────────────────────┐
                            │ PySpark Batch Job     │
                            │  (batch_analysis.py)  │
                            └───────────┬───────────┘
                                        │
                                        ▼
                            ┌───────────────────────┐
                            │  hasil_batch_saham.csv│
                            └───────────┬───────────┘
                                        │
                                        │
                                        ▼
┌─────────────────┐       ┌─────────────┴─┐       ┌──────────────────┐
│  Local Dataset  │──────▶│ Kafka Producer│──────▶│  Apache Kafka    │
│(stock_prices_   │       │  (producer.py)│       │  Topic:          │
│  daily.csv)     │       └───────────────┘       │  stock_market    │
└─────────────────┘                               └────────┬─────────┘
                                                           │
                                                           │ (Read Stream)
                                                           ▼
                                                ┌──────────────────────────┐
                                                │ PySpark Structured       │
                                                │ Streaming Job            │
                                                │ (streaming_job.py)       │
                                                └──────────┬───────────────┘
                                                           │
                                                           ▼
                                                ┌───────────────────────────┐
                                                │ data/stream_output/       │
                                                │ (JSON files, real-time)   │
                                                └──────────┬────────────────┘
                                                           │
                                                           │
                                                           ▼
                                                ┌──────────────────────┐
                                                │  Streamlit Dashboard │
                                                │     (app.py)         │
                                                └──────────────────────┘

```
┌─────────────────┐       ┌───────────────┐       ┌──────────────────┐
│  CSV Dataset    │──────▶│ Kafka Producer│──────▶│  Apache Kafka    │
│(stock_prices_   │       │  (producer.py)│       │  Topic:          │
│  daily.csv)     │       └───────────────┘       │  stock_market    │
└─────────────────┘                               └────────┬─────────┘
        │                                                  │
        │ (Batch)                                          │ (Streaming)
        ▼                                                  ▼
┌──────────────────┐                        ┌──────────────────────────┐
│  PySpark Batch   │                        │ PySpark Structured       │
│  Analysis        │                        │ Streaming Job            │
│  (batch_         │                        │ (streaming_job.py)       │
│   analysis.py)   │                        └────────────┬─────────────┘
└────────┬─────────┘                                     │
         │                                               │
         ▼                                               ▼
┌──────────────────┐                        ┌───────────────────────────┐
│ hasil_batch_     │                        │ data/stream_output/       │
│ saham.csv        │                        │ (JSON files, real-time)   │
└────────┬─────────┘                        └──────────────┬────────────┘
         │                                                 │
         └─────────────────┬───────────────────────────────┘
                           ▼
                ┌──────────────────────┐
                │  Streamlit Dashboard │
                │     (app.py)         │
                │  Port: 8501          │
                └──────────────────────┘
```

---

## Teknologi yang Digunakan

| Teknologi | Versi | Fungsi |
|---|---|---|
| Apache Kafka | 7.4.0 (Confluent) | Message broker untuk data streaming |
| Apache Zookeeper | 7.4.0 (Confluent) | Koordinasi cluster Kafka |
| Apache Spark (PySpark) | 3.3.0 | Batch & Structured Streaming processing |
| Streamlit | Latest | Dashboard visualisasi interaktif |
| Plotly | Latest | Grafik candlestick, bar chart, line chart |
| Pandas | Latest | Manipulasi data |
| Docker & Docker Compose | - | Orkestrasi layanan infrastruktur |

---

## Dataset

**File:** `data/stock_prices_daily.csv`

Dataset berisi harga saham harian dari berbagai perusahaan yang terdaftar di bursa saham AS, mulai tahun **2020 hingga 2026**.

| Kolom | Tipe | Keterangan |
|---|---|---|
| `Date` | Datetime (UTC) | Tanggal perdagangan |
| `Ticker` | String | Kode saham (contoh: `AAPL`, `GOOG`) |
| `Company_Name` | String | Nama perusahaan |
| `Sector` | String | Sektor industri (contoh: Technology, Finance) |
| `Industry` | String | Sub-industri |
| `Open` | Float | Harga pembukaan |
| `High` | Float | Harga tertinggi |
| `Low` | Float | Harga terendah |
| `Close` | Float | Harga penutupan |
| `Adj_Close` | Float | Harga penutupan yang telah disesuaikan |
| `Volume` | Integer | Volume perdagangan |

---

## Cara Menjalankan

### Prasyarat

- **Docker & Docker Compose** — untuk menjalankan Kafka dan Zookeeper
- **Python 3.9+**
- **Java 8/11** — diperlukan oleh Apache Spark
- **Apache Spark** dengan PySpark

### Langkah 1 — Jalankan Kafka dan Zookeeper Terminal 1

```bash
cd uc-bd-material
docker-compose up -d
```

Perintah ini akan menjalankan:
- **Zookeeper** di port `22181`
- **Kafka Broker** di port `9092`

### expected output
<img width="1193" height="130" alt="image" src="https://github.com/user-attachments/assets/1186e90d-9da4-4399-9140-9d16ac105d47" />


### Langkah 2 — Install Dependensi Producer dan dashboard

```bash
cd dashboard
pip install -r requirements.txt
cd ..
cd producer
pip install -r requirements.txt
```
### Langkah 3 — buat folder kosong untuk data streaming (PySpark) 

Job ini memproses seluruh dataset secara batch, menghitung rata-rata harga penutupan dan total volume per sektor per hari, lalu menyimpan hasilnya ke file CSV.

```bash
cd uc-bd-material/jobs
python streaming_job.py
```
<img width="1337" height="120" alt="image" src="https://github.com/user-attachments/assets/708d0cae-adee-4cac-9fcf-d7e9a0af9c6f" />

### Langkah 4 — Jalankan Kafka Producer (Terminal 2)

Producer akan membaca dataset CSV dan mengirimkan data saham tahun **2024–2026** ke Kafka secara kontinu (100 data/detik, loop otomatis).

```bash
buka terminal baru
python producer/producer.py
```
<img width="570" height="226" alt="image" src="https://github.com/user-attachments/assets/798fff3b-22b9-4619-a88e-b90905dd583b" />

tulisan data terkirim


### Langkah 6 — Jalankan Dashboard Streamlit (Terminal 3)

```bash
buka terminal baru
cd uc-bd-material/dashboard
streamlit run app.py
```

seharusnya bisa langsung ke direct ke website
Dashboard dapat diakses di: **http://localhost:8501**


---

## Fitur Dashboard

Dashboard Streamlit (`app.py`) menyediakan visualisasi interaktif sebagai berikut:

- ** Top 10 Gainers** — Saham dengan kenaikan harga terbesar dalam rentang waktu yang dipilih (maksimal 1 bulan)
- ** Batch Analysis** — Visualisasi hasil batch PySpark: rata-rata harga penutupan dan total volume per sektor
- ** Real-Time Streaming** — Tampilan data saham yang masuk secara real-time dari Kafka (auto-refresh setiap 3 detik)
- ** Candlestick Chart** — Grafik candlestick interaktif untuk analisis teknikal saham individual
- ** Auto-Refresh Toggle** — Aktifkan/nonaktifkan pembaruan data otomatis dari sidebar

---

## Konfigurasi Kafka (docker-compose.yml)

| Parameter | Nilai |
|---|---|
| Kafka Broker | `localhost:9092` |
| Zookeeper | `localhost:22181` |
| Topic | `stock_market` |
| Partisi | 1 |
| Replication Factor | 1 |

---

## Catatan

- **Windows users:** `streaming_job.py` menyertakan patch otomatis untuk mengunduh `winutils.exe` dan `hadoop.dll` yang diperlukan Hadoop di lingkungan Windows.
- Pastikan Kafka sudah berjalan sebelum menjalankan Producer maupun Streaming Job.
- Batch job dapat dijalankan tanpa Kafka; cukup siapkan file `stock_prices_daily.csv` di folder `data/`.

---

## Lisensi

Proyek ini dibuat untuk keperluan pembelajaran dan demonstrasi pipeline Big Data.
