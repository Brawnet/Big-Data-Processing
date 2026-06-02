import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import glob

st.set_page_config(layout="wide", page_title="Stock Market Dashboard")
st.title("📈 Advanced Stock Market Dashboard")

# Tambahkan ini di menu samping (sidebar)
st.sidebar.divider()
st.sidebar.subheader("⚙️ Pengaturan Real-Time")
auto_refresh = st.sidebar.toggle("🔄 Nyalakan Auto-Refresh (3 detik)")

# --- Fungsi Format Volume (Billion/Million) ---
def format_volume(val):
    if val >= 1_000_000_000:
        return f"{val / 1_000_000_000:.2f}B"
    elif val >= 1_000_000:
        return f"{val / 1_000_000:.2f}M"
    return str(val)

@st.cache_data
def load_batch_data():
    path = '../data/batch_results/hasil_batch_saham.csv'
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

@st.cache_data
def load_raw_data():
    path = '../data/stock_prices_daily.csv'
    if os.path.exists(path):
        df = pd.read_csv(path)
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.date
        df = df.sort_values(by='Date')
        return df
    return pd.DataFrame()

batch_df = load_batch_data()
raw_df = load_raw_data()

# --- FUNGSI LOAD DATA STREAMING (Real-Time) ---
@st.cache_data(ttl=1) 
def load_streaming_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, '../data/stream_output/*.json') 
    
    files = glob.glob(path)
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for f in files:
        try:
            # Baca file JSON baris per baris
            df = pd.read_json(f, lines=True)
            
            # VALIDASI: Hanya ambil dataframe yang TIDAK KOSONG dan punya kolom 'Date'
            if not df.empty and 'Date' in df.columns:
                dfs.append(df)
        except:
            pass
            
    # Jika setelah disaring ternyata ada datanya, baru digabung
    if dfs:
        stream_df = pd.concat(dfs, ignore_index=True)
        # Ubah format Date agar seragam dengan data Batch
        if 'Date' in stream_df.columns:
            stream_df['Date'] = pd.to_datetime(stream_df['Date'], utc=True).dt.date
        return stream_df
        
    # Jika semua file JSON ternyata kosong, kembalikan dataframe kosong
    return pd.DataFrame()

# ==========================================
# 1. TOP 10 GAINERS (Berdasarkan Rentang Waktu)
# ==========================================
if not raw_df.empty:
    st.subheader("🚀 Top 10 Gainers")
    
    # Ambil tanggal minimum dan maksimum dari data mentah
    min_date_raw = raw_df['Date'].min()
    max_date_raw = raw_df['Date'].max()
    
    # Buat default pilihan ke 7 hari terakhir
    default_start = max_date_raw - pd.Timedelta(days=7)
    if default_start < min_date_raw:
        default_start = min_date_raw

    # Menampilkan Date Picker khusus untuk Gainers
    rentang_gainer = st.date_input(
        "📅 Pilih Rentang Waktu (Maksimal 1 Bulan):",
        value=(default_start, max_date_raw),
        min_value=min_date_raw,
        max_value=max_date_raw,
        key="gainer_date_picker" # Key unik agar tidak bentrok dengan kalender di bawah
    )

    if len(rentang_gainer) == 2:
        start_date, end_date = rentang_gainer
        
        # VALIDASI: Cek apakah rentang waktu lebih dari 31 hari
        if (end_date - start_date).days > 31:
            st.error("❌ Rentang tanggal terlalu panjang! Maksimal pemilihan adalah 31 hari (1 bulan). Silakan persempit pilihan Anda.")
        else:
            # 1. Filter data berdasarkan rentang tanggal yang dipilih
            df_range = raw_df[(raw_df['Date'] >= start_date) & (raw_df['Date'] <= end_date)]
            
            if not df_range.empty:
                # 2. Cari harga Open di awal tanggal, Close di akhir tanggal, dan Total Volume
                # Karena data sudah diurutkan per tanggal di fungsi load_raw_data, kita bisa pakai first() dan last()
                gainer_agg = df_range.groupby(['Ticker', 'Company_Name']).agg(
                    Open_Start=('Open', 'first'),
                    Close_End=('Close', 'last'),
                    Total_Volume=('Volume', 'sum')
                ).reset_index()
                
                # 3. Hitung persentase kenaikan dari awal periode ke akhir periode
                gainer_agg['% Change'] = ((gainer_agg['Close_End'] - gainer_agg['Open_Start']) / gainer_agg['Open_Start']) * 100
                
                # 4. Ambil Top 10 tertinggi
                top_10 = gainer_agg.sort_values(by='% Change', ascending=False).head(10)
                
                # 5. Format tampilan kolom agar rapi
                top_10['Volume Formatted'] = top_10['Total_Volume'].apply(format_volume)
                top_10['% Change Formatted'] = top_10['% Change'].apply(lambda x: f"+{x:.2f}%" if x > 0 else f"{x:.2f}%")
                
                # 6. Susun urutan kolom yang akan ditampilkan
                top_10_display = top_10[['Ticker', 'Company_Name', 'Close_End', '% Change Formatted', 'Volume Formatted']]
                top_10_display.columns = ['Ticker', 'Company_Name', 'Close Price (End)', '% Change', 'Total Volume']
                
                # Tampilkan sebagai tabel
                st.dataframe(top_10_display, use_container_width=True)
            else:
                st.warning("Tidak ada data transaksi saham pada rentang tanggal tersebut.")
    else:
        st.info("⚠️ Silakan klik tanggal awal dan akhir untuk melihat Top 10 Gainers.")

st.divider()

# ==========================================
# 2. CANDLESTICK CHART 
# ==========================================
st.subheader("🕯️ Analisis Pergerakan Harga (Candlestick - Real-Time)")

if not raw_df.empty:
    daftar_ticker = raw_df['Ticker'].unique()
    default_idx = int(list(daftar_ticker).index('AAPL')) if 'AAPL' in daftar_ticker else 0
    pilihan_ticker = st.selectbox("Pilih Saham (Grafik akan memanjang saat data baru masuk):", daftar_ticker, index=default_idx)
    
    # --- 1. PROSES BATCH DATA (Khusus sebelum 2024) ---
    # Pastikan tipe Date konsisten
    raw_df['Date'] = pd.to_datetime(raw_df['Date'])
    raw_df['Year'] = raw_df['Date'].dt.year
    batch_data = raw_df[(raw_df['Ticker'] == pilihan_ticker) & (raw_df['Year'] < 2024)].copy()
    
    # --- 2. PROSES STREAMING DATA (Dari Spark, 2024 ke atas) ---
    stream_df = load_streaming_data()
    
    # --- 3. GABUNGKAN KEDUANYA ---
    if not stream_df.empty:
        stream_data = stream_df[stream_df['Ticker'] == pilihan_ticker].copy()
        
        # Samakan format tanggal sebelum digabung agar tidak error
        if not stream_data.empty:
            stream_data['Date'] = pd.to_datetime(stream_data['Date'])
            data_pilihan = pd.concat([batch_data, stream_data], ignore_index=True)
            st.success(f"🟢 Membaca {len(stream_data)} data live dari Spark!")
        else:
            data_pilihan = batch_data
            st.info("🟡 Data stream terbaca, tapi belum ada saham ini yang lewat.")
    else:
        data_pilihan = batch_data
        st.warning("🔴 Menunggu data live dari Spark... (Hanya menampilkan data historis)")
        
    # Urutkan dan hapus duplikat
    data_pilihan = data_pilihan.sort_values(by='Date').drop_duplicates(subset=['Date'], keep='last')
    
    # Buat Visualisasi Candlestick
    fig_candle = go.Figure(data=[go.Candlestick(
        x=data_pilihan['Date'],
        open=data_pilihan['Open'],
        high=data_pilihan['High'],
        low=data_pilihan['Low'],
        close=data_pilihan['Close'],
        name=pilihan_ticker
    )])
    
    fig_candle.update_layout(
        title=f"Candlestick Chart - {pilihan_ticker} (Termasuk Live Data Spark)", 
        yaxis_title="Harga (USD)", 
        xaxis_title="Tanggal",
        xaxis_rangeslider_visible=True # Menambahkan slider zoom di bawah grafik
    )
    st.plotly_chart(fig_candle, use_container_width=True)

st.divider()
# ==========================================
# 3. BATCH VISUALIZATION (BARPLOT SEKTOR - RENTANG TANGGAL)
# ==========================================
st.subheader("📊 Analisis Batch: Tren Sektor & Volume")

if not batch_df.empty:
    # 1. Pastikan kolom tanggal berformat date
    batch_df['Date_Only'] = pd.to_datetime(batch_df['Date_Only']).dt.date
    min_date = batch_df['Date_Only'].min()
    max_date = batch_df['Date_Only'].max()
    
    # 2. Kalender
    rentang_tanggal = st.date_input(
        "📅 Pilih Rentang Tanggal (Klik 2x: Mulai & Selesai):",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="batch_date_picker"
    )
    
    # 3. Cek apakah user sudah mengklik tanggal awal DAN akhir
    if len(rentang_tanggal) == 2:
        tgl_awal, tgl_akhir = rentang_tanggal
        batch_filter = batch_df[(batch_df['Date_Only'] >= tgl_awal) & (batch_df['Date_Only'] <= tgl_akhir)]
        
        if not batch_filter.empty:
            batch_agg = batch_filter.groupby("Sector").agg(
                Avg_Close_Price=("Avg_Close_Price", "mean"), 
                Total_Volume=("Total_Volume", "sum")         
            ).reset_index()
            
            # --- CARA TERAMAN: Bagi langsung data volume dengan 1 Miliar ---
            # Kita buat kolom baru bernama Total_Volume_Billion
            batch_agg['Total_Volume_Billion'] = batch_agg['Total_Volume'] / 1_000_000_000
            
            col1, col2 = st.columns(2)
            with col1:
                fig_bar_price = px.bar(batch_agg, x="Sector", y="Avg_Close_Price", color="Sector",
                                       title=f"Rata-rata Harga Saham per Sektor<br>({tgl_awal} s/d {tgl_akhir})",
                                       labels={"Avg_Close_Price": "Harga Rata-rata (USD)", "Sector": "Sektor"})
                fig_bar_price.update_layout(showlegend=False)
                st.plotly_chart(fig_bar_price, use_container_width=True)
                
            with col2:
                # Gunakan kolom volume yang sudah dibagi 1 Miliar
                fig_bar_vol = px.bar(batch_agg, x="Sector", y="Total_Volume_Billion", color="Sector",
                                     title=f"Total Volume Perdagangan per Sektor<br>({tgl_awal} s/d {tgl_akhir})",
                                     labels={"Total_Volume_Billion": "Total Volume (Billion/Miliar)", "Sector": "Sektor"})
                
                # Format angka biasa (2 angka di belakang koma), tanpa singkatan SI "G"
                fig_bar_vol.update_layout(yaxis=dict(tickformat=".2f"), showlegend=False)
                st.plotly_chart(fig_bar_vol, use_container_width=True)
        else:
            st.warning("Tidak ada data pada rentang tanggal tersebut.")
    else:
        # Jika grafik menghilang, pesan ini akan muncul untuk mengingatkan user
        st.info("⚠️ Silakan klik tanggal AKHIR di kalender untuk memunculkan grafik.")
else:
    st.warning("Data Batch belum tersedia.")

# ==========================================
# MESIN PENGGERAK AUTO-REFRESH
# ==========================================
if auto_refresh:
    import time
    time.sleep(3) # Jeda waktu sebelum layar berkedip (3 detik)
    st.rerun()    # Perintah sakti untuk me-refresh Streamlit otomatis