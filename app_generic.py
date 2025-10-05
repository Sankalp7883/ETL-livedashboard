import sqlite3
import pandas as pd
import streamlit as st

DB_FILE = "datawarehouse.db"

# -------------------------------
# Utility: Get all tables
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

# Sidebar for data source
st.sidebar.header("📁 Data Source")
source_option = st.sidebar.radio("Select source:", ["Existing DB Table", "Upload File"])

df = None

if source_option == "Existing DB Table":
    tables = get_table_names()
    if not tables:
        st.error("⚠️ No tables found in the database. Please run `load_generic.py` first.")
        st.stop()
    table_choice = st.sidebar.selectbox("Choose a table:", tables)
    df = load_table(table_choice)
    st.success(f"✅ Loaded table: `{table_choice}`")

elif source_option == "Upload File":
    uploaded = st.sidebar.file_uploader("📤 Upload Excel or CSV", type=["xlsx", "csv"])
    if uploaded:
        try:
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded)
            elif uploaded.name.endswith(".csv"):
                df = pd.read_csv(uploaded)
            st.success(f"✅ File uploaded: `{uploaded.name}`")
        except Exception as e:
            st.error(f"Error reading file: {e}")

# Stop if no data
if df is None:
    st.info("👈 Please select or upload a dataset to begin.")
    st.stop()

# Clean data
df = df.replace([float("inf"), -float("inf")], pd.NA)
df = df.dropna(how="all")

# -------------------------------
# Data Preview
# -------------------------------
st.subheader("🔍 Data Preview")
st.dataframe(df.head(50), use_container_width=True)

# -------------------------------
# Column Summary
# -------------------------------
st.subheader("📋 Column Summary")
info = pd.DataFrame({
    "Column": df.columns,
    "Data Type": df.dtypes.astype(str),
    "Missing Values": df.isna().sum(),
})
st.dataframe(info, use_container_width=True)

# -------------------------------
# Simple Filters (no fancy ones)
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        unique_vals = df[col].dropna().unique().tolist()
        if 1 < len(unique_vals) <= 50:
            selected_vals = st.sidebar.multiselect(f"{col}:", unique_vals)
            if selected_vals:
                filtered_df = filtered_df[filtered_df[col].isin(selected_vals)]

# -------------------------------
# Charts
# -------------------------------
st.subheader("📈 Data Visualization")

numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
chart_type = st.selectbox("Select chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])

if chart_type != "None" and not filtered_df.empty and numeric_cols:
    x_axis = st.selectbox("X-axis:", filtered_df.columns)
    y_axis = st.selectbox("Y-axis (numeric):", numeric_cols)

    try:
        if chart_type == "Histogram":
            st.bar_chart(filtered_df[y_axis].dropna())
        elif chart_type == "Line Chart":
            if x_axis in filtered_df.columns:
                st.line_chart(filtered_df.set_index(x_axis)[y_axis])
        elif chart_type == "Bar Chart":
            grouped = filtered_df.groupby(x_axis, dropna=False)[y_axis].sum().reset_index()
            st.bar_chart(grouped.set_index(x_axis))
    except Exception as e:
        st.warning(f"⚠️ Could not render chart: {e}")

# -------------------------------
# Download Filtered Data
# -------------------------------
st.subheader("💾 Download Filtered Data")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download as CSV",
    data=csv,
    file_name="filtered_data.csv",
    mime="text/csv"
)
