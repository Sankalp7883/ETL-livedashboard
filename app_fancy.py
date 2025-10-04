
import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, date

st.set_page_config(page_title="Bookstore Inventory — Advanced Dashboard", layout="wide")

# ---------- Helpers ----------
def load_data(db_path="financial_data.db", excel_path="data/ABC_Book_Stores_Inventory_Register.xlsx"):
    """Try reading from DB first; if not available, try reading the Excel directly (convenience)."""
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql("SELECT * FROM financial_data", conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Failed reading DB: {e}")
    # Fallback: try reading Excel directly
    if os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path, sheet_name="Inventory Register")
            return df
        except Exception as e:
            st.error(f"Failed reading Excel {excel_path}: {e}")
    st.warning("No data found. Please run the ETL to create `financial_data.db` or upload an Excel file.")
    return pd.DataFrame()

def canonicalize_columns(df):
    """Normalize column names to lowercase_no_spaces and map known aliases to canonical names."""
    if df is None or df.empty:
        return df
    df = df.copy()
    # Normalize
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {}
    # Common aliases mapping (non-exhaustive)
    aliases = {
        'date': ['date of sales invoice','date','transaction_date','date_of_sales_invoice','date of sales invoice'],
        'opening_stock': ['issued from the opening stock','opening_stock','issued_from_the_opening_stock','issued_from_opening_stock'],
        'purchases': ['issued from the current year','issued from the current year','purchases','issued_from_the_current_year'],
        'revenue': ['total amount','total_amount','revenue','amount','total'],
        'closing_stock_units': ['closing stock no. of units','closing stock no. of units','closing_stock_no_of_units','closing_stock_units'],
        'closing_stock_value': ['closing stock total amount','closing stock total amount','closing_stock_total_amount','closing_stock_value'],
        'book_title': ['book title','book_title','title'],
        'author': ['author','authors'],
        'category': ['category','book category'],
    }
    # lower mapping
    lower_cols = {c.lower(): c for c in df.columns}
    for canonical, keys in aliases.items():
        for key in keys:
            if key in lower_cols:
                col_map[lower_cols[key]] = canonical
                break
        else:
            # try substring match
            for lc, orig in lower_cols.items():
                for key in keys:
                    if key in lc and orig not in col_map:
                        col_map[orig] = canonical
                        break
    # apply mapping
    df = df.rename(columns=col_map)
    # Convert known numeric columns
    for col in ['opening_stock','purchases','revenue','closing_stock_units','closing_stock_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # Parse date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df

def format_inr(amount):
    """Format a number in INR with lakhs/crores shorthand when large."""
    try:
        a = float(amount)
    except:
        return "₹ 0"
    sign = "-" if a < 0 else ""
    a_abs = abs(a)
    if a_abs >= 1e7:
        return f"{sign}₹ {a_abs/1e7:,.2f} Cr"
    if a_abs >= 1e5:
        return f"{sign}₹ {a_abs/1e5:,.2f} L"
    return f"{sign}₹ {a_abs:,.2f}"

def format_int(x):
    try:
        return f"{int(x):,}"
    except:
        return "0"

# ---------- Load & Prep ----------
st.title("📚 Bookstore Inventory — Advanced Dashboard")
st.write("Interactive high-level KPIs, trends, top-sellers and drilldowns.")

df_raw = load_data()
if df_raw.empty:
    # allow upload
    uploaded = st.file_uploader("Upload Excel (Inventory Register) to preview", type=["xlsx","xls"])
    if uploaded is not None:
        try:
            df_raw = pd.read_excel(uploaded, sheet_name="Inventory Register")
            st.success("Loaded uploaded file")
        except Exception as e:
            st.error(f"Failed reading uploaded Excel: {e}")

if df_raw is None or df_raw.empty:
    st.stop()

df = canonicalize_columns(df_raw)

st.sidebar.header("Filters & Controls")

# Date range filter
if 'date' in df.columns and not df['date'].dropna().empty:
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    start_date, end_date = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    # ensure ordering
    if isinstance(start_date, list) or isinstance(start_date, tuple):
        start_date, end_date = start_date[0], start_date[1]
    if start_date > end_date:
        start_date, end_date = end_date, start_date
else:
    # fallback to entire data
    start_date = None
    end_date = None

# Category & Author filters (multi-select)
category_options = ['All'] + sorted([str(x) for x in df['category'].dropna().unique()]) if 'category' in df.columns else ['All']
author_options = ['All'] + sorted([str(x) for x in df['author'].dropna().unique()]) if 'author' in df.columns else ['All']

selected_categories = st.sidebar.multiselect("Category", options=category_options, default=['All'])
selected_authors = st.sidebar.multiselect("Author", options=author_options, default=['All'])

# Book search
book_search = st.sidebar.text_input("Search Book Title (substring)")

# Top N slider
top_n = st.sidebar.slider("Top N Bestsellers", min_value=5, max_value=20, value=10, step=1)

# Apply filters
df_filtered = df.copy()
if start_date and end_date and 'date' in df_filtered.columns:
    df_filtered = df_filtered[(df_filtered['date'].dt.date >= start_date) & (df_filtered['date'].dt.date <= end_date)]

if 'category' in df_filtered.columns and 'All' not in selected_categories:
    df_filtered = df_filtered[df_filtered['category'].isin(selected_categories)]

if 'author' in df_filtered.columns and 'All' not in selected_authors:
    df_filtered = df_filtered[df_filtered['author'].isin(selected_authors)]

if book_search:
    df_filtered = df_filtered[df_filtered['book_title'].str.contains(book_search, case=False, na=False) | df_filtered['book_title'].isna()] if 'book_title' in df_filtered.columns else df_filtered

# ---------- KPIs ----------
st.header("Key Metrics")
k1, k2, k3, k4 = st.columns(4)

total_transactions = len(df_filtered)
total_opening = df_filtered['opening_stock'].sum() if 'opening_stock' in df_filtered.columns else 0
total_closing_units = df_filtered['closing_stock_units'].sum() if 'closing_stock_units' in df_filtered.columns else 0
total_revenue = df_filtered['revenue'].sum() if 'revenue' in df_filtered.columns else 0.0

k1.metric("Total Transactions", format_int(total_transactions))
k2.metric("Total Opening Stock", format_int(total_opening))
k3.metric("Total Closing Stock (units)", format_int(total_closing_units))
k4.metric("Total Revenue", format_inr(total_revenue))

st.markdown(f"**Showing rows:** {len(df_filtered):,} (filtered from {len(df):,})")
st.markdown("Loaded columns: " + ", ".join(df.columns.tolist()))

# ---------- Trends ----------
st.subheader("Trends Over Time")
colA, colB = st.columns(2)

with colA:
    if 'date' in df_filtered.columns and 'revenue' in df_filtered.columns and not df_filtered['date'].dropna().empty:
        rev_month = df_filtered.set_index('date').resample('M')['revenue'].sum()
        st.markdown("**Revenue (monthly)**")
        st.line_chart(rev_month)
    else:
        st.info("Revenue or date column not available for revenue trend.")

with colB:
    stock_cols = [c for c in ['opening_stock','closing_stock_units'] if c in df_filtered.columns]
    if 'date' in df_filtered.columns and stock_cols:
        stock_month = df_filtered.set_index('date')[stock_cols].resample('M').sum()
        st.markdown("**Stock (monthly)**")
        st.line_chart(stock_month)
    else:
        st.info("Stock columns or date missing for stock trend.")

# ---------- Top N Bestsellers ----------
st.subheader(f"Top {top_n} Bestsellers (by Revenue)")
if 'book_title' in df_filtered.columns and 'revenue' in df_filtered.columns:
    top_books = df_filtered.groupby('book_title', dropna=False)['revenue'].sum().sort_values(ascending=False).head(top_n)
    st.bar_chart(top_books)
else:
    st.info("Book title or revenue column missing; cannot compute bestsellers.")

# ---------- Drilldown ----------
st.subheader("Per-book Drilldown")
if 'book_title' in df_filtered.columns:
    book_list = sorted([str(x) for x in df_filtered['book_title'].dropna().unique()])
    selected_book = st.selectbox("Select a book to drill down", options=["(none)"] + book_list, index=0)
    if selected_book and selected_book != "(none)":
        book_df = df_filtered[df_filtered['book_title'] == selected_book]
        st.markdown(f"**{selected_book}** — Rows: {len(book_df):,}")
        # Book KPIs
        bcol1, bcol2, bcol3 = st.columns(3)
        bcol1.metric("Total Revenue", format_inr(book_df['revenue'].sum()) if 'revenue' in book_df.columns else "N/A")
        bcol2.metric("Total Issued (Opening + Purchases)", format_int((book_df.get('opening_stock',0).sum() + book_df.get('purchases',0).sum())))
        bcol3.metric("Closing Stock (units)", format_int(book_df['closing_stock_units'].sum()) if 'closing_stock_units' in book_df.columns else "N/A")
        # Time series for the selected book
        if 'date' in book_df.columns and 'revenue' in book_df.columns and not book_df['date'].dropna().empty:
            book_ts = book_df.set_index('date').resample('M')['revenue'].sum()
            st.line_chart(book_ts)
        else:
            st.info("Insufficient date/revenue data for this book to show time series.")
else:
    st.info("No book_title column available for drilldown.")

# ---------- Download filtered data ----------
st.subheader("Download Data")
if not df_filtered.empty:
    csv_bytes = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button("Download filtered data as CSV", data=csv_bytes, file_name="filtered_inventory.csv", mime="text/csv")
else:
    st.info("No data to download for the current filters.")

# ---------- Raw Data Preview ----------
st.subheader("Data Preview (first 200 rows)")
st.dataframe(df_filtered.head(200))
