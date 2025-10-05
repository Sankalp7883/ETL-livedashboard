import sqlite3
import pandas as pd
import streamlit as st

DB_FILE = "datawarehouse.db"

# -------------------------------
# Utility: Get all tables from DB
# -------------------------------
def get_table_names():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cursor.fetchall()]
        return tables
    except Exception:
        return []

# -------------------------------
# Load a table into DataFrame
# -------------------------------
def load_table(table_name):
    with sqlite3.connect(DB_FILE) as conn:
        return pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)

# -------------------------------
# Streamlit UI
# -------------------------------
st.set_page_config(page_title="ETL Live Dashboard", layout="wide")
st.title("📊 ETL Live Dashboard")

# Sidebar options
st.sidebar.header("📁 Data Source")
source_option = st.sidebar.radio("Choose data source:", ["Existing DB Table", "Upload File"])

df = None

if source_option == "Existing DB Table":
    tables = get_table_names()
    if not tables:
        st.error("⚠️ No tables found in the database. Please run `load_generic.py` first.")
        st.stop()

    table_choice = st.sidebar.selectbox("Select a table:", tables)
    df = load_table(table_choice)
    st.success(f"✅ Loaded table: `{table_choice}`")

elif source_option == "Upload File":
    uploaded = st.sidebar.file_uploader("📤 Upload Excel or CSV file", type=["xlsx", "csv"])
    if uploaded:
        try:
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded)
            elif uploaded.name.endswith(".csv"):
                df = pd.read_csv(uploaded)
        except Exception as e:
            st.error(f"Error reading file: {e}")

# Stop if no data
if df is None:
    st.info("👈 Please upload a file or choose an existing table to start.")
    st.stop()

# Clean data
df = df.replace([float('inf'), -float('inf')], pd.NA)
df = df.dropna(how="all")

# -------------------------------
# Data Preview
# -------------------------------
st.subheader("🔍 Data Preview")
st.dataframe(df.head(50), width='stretch')

# -------------------------------
# Column Summary
# -------------------------------
st.subheader("📋 Column Summary")
info = pd.DataFrame({
    "Column": df.columns,
    "Data Type": df.dtypes.astype(str),
    "Missing Values": df.isna().sum(),
})
st.dataframe(info, width='stretch')

# -------------------------------
# Filters (Simple Default)
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        values = df[col].dropna().unique().tolist()
        if len(values) > 0 and len(values) <= 50:
            choice = st.sidebar.multiselect(f"{col}", values)
            if choice:
                filtered_df = filtered_df[filtered_df[col].isin(choice)]

# -------------------------------
# Charts Section
# -------------------------------
st.markdown("### 📈 Charts")

numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
date_cols = filtered_df.select_dtypes(include=["datetime64"]).columns.tolist()

chart_type = st.selectbox("Choose chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])

if chart_type != "None":
    x_axis = st.selectbox("X-axis:", numeric_cols + date_cols + filtered_df.columns.tolist())
    y_axis = st.selectbox("Y-axis (numeric only):", numeric_cols)

    if chart_type == "Histogram":
        st.bar_chart(filtered_df[y_axis])
    elif chart_type == "Line Chart":
        st.line_chart(filtered_df.set_index(x_axis)[y_axis])
    elif chart_type == "Bar Chart":
        grouped = filtered_df.groupby(x_axis)[y_axis].sum().reset_index()
        st.bar_chart(grouped.set_index(x_axis))

# -------------------------------
# Download filtered data
# -------------------------------
st.markdown("### 💾 Download Data")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download as CSV",
    csv,
    f"{table_choice}_filtered.csv",
    "text/csv",
    key="download-csv"
)
