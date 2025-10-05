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
# Streamlit App Config
# -------------------------------
st.set_page_config(page_title="ETL Live Dashboard", layout="wide")
st.title("📊 ETL Live Dashboard")

# Sidebar source selector
st.sidebar.header("📁 Data Source")
source_option = st.sidebar.radio("Choose data source:", ["Existing DB Table", "Upload File"])

df = None

# -------------------------------
# Load from DB or File
# -------------------------------
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
            st.success(f"✅ Successfully loaded: {uploaded.name}")
        except Exception as e:
            st.error(f"❌ Error reading file: {e}")

# Stop if no data
if df is None:
    st.info("👈 Please upload a file or choose an existing table to start.")
    st.stop()

# Clean data
df = df.replace([float('inf'), -float('inf')], pd.NA)
df = df.dropna(how="all")
df.columns = [str(c).strip() for c in df.columns]

# -------------------------------
# Data Preview
# -------------------------------
st.subheader("🔍 Data Preview")
st.dataframe(df.head(50), width='stretch')

# -------------------------------
# Column Summary
# -------------------------------
def readable_dtype(dtype):
    if "int" in str(dtype) or "float" in str(dtype):
        return "Numeric"
    elif "datetime" in str(dtype):
        return "Date"
    else:
        return "Text"

st.subheader("📋 Column Summary")
info = pd.DataFrame({
    "Column": df.columns,
    "Data Type": [readable_dtype(t) for t in df.dtypes],
    "Missing Values": df.isna().sum(),
})
st.dataframe(info, width='stretch')

# -------------------------------
# Filters (basic)
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        values = df[col].dropna().unique().tolist()
        if 1 < len(values) <= 50:
            selected = st.sidebar.multiselect(f"{col}", values)
            if selected:
                filtered_df = filtered_df[filtered_df[col].isin(selected)]

# -------------------------------
# Charts Section (Safe)
# -------------------------------
st.markdown("### 📈 Charts")

numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
all_cols = filtered_df.columns.tolist()

chart_type = st.selectbox("Choose chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])

if chart_type != "None" and not filtered_df.empty:
    x_axis = st.selectbox("X-axis:", all_cols)
    y_axis = st.selectbox("Y-axis (numeric only):", numeric_cols)

    if y_axis not in filtered_df.columns or x_axis not in filtered_df.columns:
        st.warning("⚠️ Selected columns are invalid for this dataset. Please choose again.")
    else:
        try:
            if chart_type == "Histogram":
                st.bar_chart(filtered_df[y_axis])
            elif chart_type == "Line Chart":
                temp_df = filtered_df[[x_axis, y_axis]].dropna()
                if not temp_df.empty:
                    st.line_chart(temp_df.set_index(x_axis)[y_axis])
                else:
                    st.warning("⚠️ Not enough valid data to plot.")
            elif chart_type == "Bar Chart":
                grouped = filtered_df.groupby(x_axis, dropna=False)[y_axis].sum().reset_index()
                st.bar_chart(grouped.set_index(x_axis))
        except Exception as e:
            st.error(f"Chart rendering failed: {e}")
else:
    st.info("Select chart type and columns to visualize your data.")

# -------------------------------
# Download Section
# -------------------------------
st.subheader("💾 Download Filtered Data")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download as CSV",
    data=csv,
    file_name="filtered_data.csv",
    mime="text/csv"
)
