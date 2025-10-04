
import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(page_title="Bookstore Inventory Dashboard", layout="wide")
st.title("📚 Bookstore Inventory — High-level Dashboard")

DB_PATH = "financial_data.db"

def load_db(path=DB_PATH):
    if not os.path.exists(path):
        st.error(f"Database not found at: {path}. Run load.py to create it.")
        return pd.DataFrame()
    conn = sqlite3.connect(path)
    df = pd.read_sql("SELECT * FROM financial_data", conn)
    conn.close()
    return df

df = load_db(DB_PATH)

st.markdown(\"**Loaded columns:**\")
st.write(df.columns.tolist())

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Transactions", len(df))
col2.metric("Total Opening Stock", int(df['opening_stock'].sum()) if 'opening_stock' in df.columns else 0)
col3.metric("Total Closing Stock (units)", int(df['closing_stock_units'].sum()) if 'closing_stock_units' in df.columns else 0)
col4.metric("Total Revenue (₹)", float(df['revenue'].sum()) if 'revenue' in df.columns else 0.0)

st.header("Trends Over Time")
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Group by month for nicer trends
    df['month'] = df['date'].dt.to_period('M').dt.to_timestamp()
    if 'revenue' in df.columns:
        rev_trend = df.groupby('month')['revenue'].sum()
        st.subheader("Revenue Trend (monthly)")
        st.line_chart(rev_trend)
    stock_cols = [c for c in ['opening_stock','closing_stock_units'] if c in df.columns]
    if stock_cols:
        stock_trend = df.groupby('month')[stock_cols].sum()
        st.subheader("Stock Trend (monthly)")
        st.line_chart(stock_trend)
else:
    st.info("No 'date' column found to build trends.")

st.header("Data Preview (first 100 rows)")
st.dataframe(df.head(100))
