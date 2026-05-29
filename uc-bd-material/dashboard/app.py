import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(layout="wide")
st.title("📈 Stock Market Dashboard")

# --- FUNGSI LOAD DATA BATCH (Sektor) ---
@st.cache_data
def load_batch_data():
    path = '../data/batch_results/hasil_batch_saham.csv'
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

# --- FUNGSI LOAD DATA MENTAH (Untuk AAPL dll) ---
@st.cache_data
def load_raw_data():
    path = '../data/stock_prices_daily.csv'
    if os.path.exists(path):
        df = pd.read_csv(path)
        # Rapikan format tanggal
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.date
        return df
    return pd.DataFrame()

batch_df = load_batch_data()
raw_df = load_raw_data()

# =========================================================
# BAGIAN 1: GRAFIK PERGERAKAN HARGA SAHAM (TICKER SPESIFIK)
# =========================================================
st.header("🔍 Pergerakan Harga Saham")

if not raw_df.empty:
    # 1. Ambil semua nama Ticker yang ada di data
    daftar_ticker = raw_df['Ticker'].unique()
    
    # 2. Cari AAPL agar jadi pilihan default saat web dibuka
    default_idx = int(list(daftar_ticker).index('AAPL')) if 'AAPL' in daftar_ticker else 0
    
    # 3. Buat menu Dropdown untuk memilih Ticker
    pilihan_ticker = st.selectbox("Pilih Simbol Saham (Ticker):", daftar_ticker, index=default_idx)
    
    # 4. Filter data HANYA untuk Ticker yang dipilih
    data_pilihan = raw_df[raw_df['Ticker'] == pilihan_ticker]
    
    # 5. Buat Grafik Garis (Line Chart) pergerakan harga Close
    fig_line = px.line(data_pilihan, x="Date", y="Close", 
                       title=f"Pergerakan Harga Penutupan (Close) - {pilihan_ticker}",
                       labels={"Close": "Harga (USD)", "Date": "Tanggal"})
    
    # Beri warna garis yang keren
    fig_line.update_traces(line_color='#17BECF') 
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.warning("Data mentah saham tidak ditemukan di folder data.")

st.divider() # Garis pemisah

# =========================================================
# BAGIAN 2: ANALISIS SEKTOR (SEPERTI SEBELUMNYA)
# =========================================================
st.header("📊 Analisis Historis: Sektor & Industri")

if not batch_df.empty:
    batch_df = batch_df.dropna(subset=['Sector', 'Industry'])
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafik 1: Rata-rata Harga Saham (Close) per Sektor
        sector_price = batch_df.groupby("Sector")["Avg_Close_Price"].mean().reset_index()
        fig_price = px.bar(sector_price, x="Sector", y="Avg_Close_Price", 
                           title="Rata-rata Harga Saham per Sektor", color="Sector")
        st.plotly_chart(fig_price, use_container_width=True)
        
    with col2:
        # Grafik 2: Total Volume Perdagangan per Sektor
        sector_vol = batch_df.groupby("Sector")["Total_Volume"].sum().reset_index()
        fig_vol = px.pie(sector_vol, names="Sector", values="Total_Volume", 
                         title="Distribusi Volume Perdagangan per Sektor")
        st.plotly_chart(fig_vol, use_container_width=True)
else:
    st.warning("Belum ada data batch. Silakan jalankan `jobs/batch_analysis.py` terlebih dahulu!")