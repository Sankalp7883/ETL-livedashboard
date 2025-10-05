import sqlite3
import pandas as pd
import streamlit as st
import os
from io import BytesIO

DB_FILE = "datawarehouse.db"

# -------------------------------
# Helper: Load tables from DB
# -------------------------------
def get_table_names():
    if not os.path.exists(DB_FILE):
        return []
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
    return tables

def load_table(table_name):
    with sqlite3.connect(DB_FILE) as conn:
        return pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)

# -------------------------------
# Streamlit Layout
# -------------------------------
st.set_page_config(page_title="ETL Live Dashboard", layout="wide")
st.title("📊 ETL Live Dashboard")
st.caption("Explore existing datasets or upload your own Excel, CSV, or PDF files.")

# -------------------------------
# Section 1: Existing database tables
# -------------------------------
st.sidebar.header("📦 Existing Data in Database")

tables = get_table_names()
selected_df = None

if tables:
    table_choice = st.sidebar.selectbox("Select a table from database:", tables)
    df = load_table(table_choice)
    st.subheader(f"🧾 Preview: `{table_choice}`")
    st.dataframe(df.head(50))

    st.markdown("### 🔍 Column Info")
    info = pd.DataFrame({
        "column": df.columns,
        "dtype": df.dtypes.astype(str),
        "missing_values": df.isna().sum(),
    })
    st.dataframe(info)

    selected_df = df
else:
    st.info("No tables found in the database yet. Upload files to create new datasets.")

# -------------------------------
# Section 2: Upload new file
# -------------------------------
st.sidebar.header("📤 Upload New Data")
uploaded = st.sidebar.file_uploader("Upload Excel, CSV, or PDF file", type=["xlsx", "csv", "pdf"])

if uploaded:
    st.subheader(f"Uploaded file: `{uploaded.name}`")
    if uploaded.name.endswith(".csv"):
        new_df = pd.read_csv(uploaded)
    elif uploaded.name.endswith(".xlsx"):
        new_df = pd.read_excel(uploaded)
    else:
        st.warning("📄 PDF extraction not yet supported in this version.")
        new_df = None

    if new_df is not None:
        st.dataframe(new_df.head(50))
        st.success("✅ File loaded successfully!")

        # Optional: Save uploaded data to DB
        if st.checkbox("Save this uploaded data to database"):
            table_name = f"user_upload_{uploaded.name.replace('.', '_')}"
            with sqlite3.connect(DB_FILE) as conn:
                new_df.to_sql(table_name, conn, if_exists="replace", index=False)
            st.success(f"Saved to database as `{table_name}`")

        selected_df = new_df

# -------------------------------
# Section 3: Visualization
# -------------------------------
if selected_df is not None:
    st.markdown("### 📈 Visualization")

    numeric_cols = selected_df.select_dtypes(include=["number"]).columns.tolist()
    date_cols = selected_df.select_dtypes(include=["datetime64"]).columns.tolist()

    chart_type = st.selectbox("Choose chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])

    if chart_type != "None":
        x_axis = st.selectbox("X-axis:", selected_df.columns)
        y_axis = st.selectbox("Y-axis (numeric only):", numeric_cols)

        if y_axis in selected_df.columns:
            try:
                if chart_type == "Histogram":
                    st.bar_chart(selected_df[y_axis])
                elif chart_type == "Line Chart":
                    st.line_chart(selected_df.set_index(x_axis)[y_axis])
                elif chart_type == "Bar Chart":
                    grouped = selected_df.groupby(x_axis)[y_axis].sum().reset_index()
                    st.bar_chart(grouped.set_index(x_axis))
            except Exception as e:
                st.warning(f"⚠️ Could not render chart: {e}")

    # -------------------------------
    # Download filtered data
    # -------------------------------
    st.markdown("### 💾 Download Data")
    csv = selected_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download as CSV",
        csv,
        f"{uploaded.name if uploaded else table_choice}_data.csv",
        "text/csv",
        key="download-csv"
    )
