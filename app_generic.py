import sqlite3
import pandas as pd
import streamlit as st

DB_FILE = "datawarehouse.db"

# -------------------------------
# Load table names dynamically
# -------------------------------
def get_table_names():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
    return tables

# -------------------------------
# Load a table into a DataFrame
# -------------------------------
def load_table(table_name):
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)

    # --- Convert timestamp-like integers to readable datetime ---
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            try:
                if df[col].max() > 1e15 and df[col].min() > 1e12:  # Likely nanoseconds
                    df[col] = pd.to_datetime(df[col], unit='ns', errors='ignore')
                elif df[col].max() > 1e9 and df[col].min() > 1e8:  # Likely seconds
                    df[col] = pd.to_datetime(df[col], unit='s', errors='ignore')
            except Exception:
                pass
    return df

# -------------------------------
# Streamlit App Layout
# -------------------------------
st.set_page_config(page_title="ETL Flexible Dashboard", layout="wide")
st.title("📊 ETL Flexible Dashboard")

# -------------------------------
# Load Tables
# -------------------------------
tables = get_table_names()
if not tables:
    st.error("❌ No tables found in database. Please run `load_generic.py` first.")
    st.stop()

table_choice = st.selectbox("Choose a table to explore:", tables)
df = load_table(table_choice)

# -------------------------------
# Data Preview
# -------------------------------
st.subheader(f"📄 Preview of `{table_choice}`")
st.dataframe(df.head(50))

# -------------------------------
# Column Information
# -------------------------------
st.markdown("### 🧾 Column Information")
info = pd.DataFrame({
    "Column": df.columns,
    "Data Type": df.dtypes.astype(str),
    "Missing Values": df.isna().sum(),
})
st.dataframe(info)

# -------------------------------
# Sidebar Filters
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        values = df[col].dropna().unique().tolist()
        if 1 < len(values) <= 50:  # manageable filter options
            choice = st.sidebar.multiselect(f"{col}", values)
            if choice:
                filtered_df = filtered_df[filtered_df[col].isin(choice)]

# -------------------------------
# Charts
# -------------------------------
st.markdown("### 📈 Data Visualization")

numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
datetime_cols = filtered_df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
all_cols = df.columns.tolist()

chart_type = st.selectbox("Choose chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])

if chart_type != "None":
    x_axis = st.selectbox("Select X-axis:", all_cols, key="xaxis")
    y_axis = st.selectbox("Select Y-axis (numeric):", numeric_cols, key="yaxis")

    if x_axis not in filtered_df.columns or y_axis not in filtered_df.columns:
        st.warning("⚠️ Please select valid X and Y columns.")
    else:
        try:
            if chart_type == "Histogram":
                st.bar_chart(filtered_df[y_axis])
            elif chart_type == "Line Chart":
                st.line_chart(filtered_df.set_index(x_axis)[y_axis])
            elif chart_type == "Bar Chart":
                grouped = filtered_df.groupby(x_axis, dropna=False)[y_axis].sum().reset_index()
                st.bar_chart(grouped.set_index(x_axis))
        except Exception as e:
            st.error(f"Chart rendering failed: {e}")

# -------------------------------
# Download filtered data
# -------------------------------
st.markdown("### 💾 Download Data")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️ Download as CSV",
    csv,
    f"{table_choice}_filtered.csv",
    "text/csv",
    key="download-csv"
)
