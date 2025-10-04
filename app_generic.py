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
        return pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)

# -------------------------------
# App layout
# -------------------------------
st.set_page_config(page_title="ETL Dashboard", layout="wide")
st.title("📊 Flexible ETL Dashboard")

# Pick a table
tables = get_table_names()
if not tables:
    st.error("No tables found in database. Please run `load_generic.py` first.")
    st.stop()

table_choice = st.selectbox("Choose a table to explore:", tables)
df = load_table(table_choice)

st.subheader(f"Preview of `{table_choice}`")
st.dataframe(df.head(50))

# Column info
st.markdown("### Column Information")
info = pd.DataFrame({
    "column": df.columns,
    "dtype": df.dtypes.astype(str),
    "missing_values": df.isna().sum(),
})
st.dataframe(info)

# -------------------------------
# Filters
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        values = df[col].dropna().unique().tolist()
        if len(values) > 0 and len(values) <= 50:  # only for categorical with manageable size
            choice = st.sidebar.multiselect(f"{col}", values)
            if choice:
                filtered_df = filtered_df[filtered_df[col].isin(choice)]

# -------------------------------
# Charts
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
