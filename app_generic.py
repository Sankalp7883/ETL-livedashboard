import sqlite3
import pandas as pd
import streamlit as st
import io
import os
from datetime import timedelta
import google.generativeai as genai

from dotenv import load_dotenv
load_dotenv()


# -------------------------------
# Configuration
# -------------------------------
DB_FILE = "datawarehouse.db"

# Load Gemini API key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY", "")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")
else:
    model = None

# -------------------------------
# Utility Functions
# -------------------------------
def get_table_names():
    """Get all tables from SQLite DB."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [r[0] for r in cursor.fetchall()]
    except Exception:
        return []

def load_table(table_name):
    """Load selected table into DataFrame."""
    with sqlite3.connect(DB_FILE) as conn:
        return pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)

# -------------------------------
# Streamlit Layout
# -------------------------------
st.set_page_config(page_title="ETL Live Dashboard", layout="wide")

# --- Header ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("📊 ETL Live Dashboard")
with col2:
    st.image("https://cdn-icons-png.flaticon.com/512/4712/4712107.png", width=70)

# --- Sidebar: Data Source ---
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
    uploaded = st.sidebar.file_uploader("📤 Upload Excel, CSV or PDF file", type=["xlsx", "csv"])
    if uploaded:
        try:
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded)
            elif uploaded.name.endswith(".csv"):
                df = pd.read_csv(uploaded)
        except Exception as e:
            st.error(f"Error reading file: {e}")

# Stop if no data loaded
if df is None:
    st.info("👈 Please upload a file or choose an existing table to start.")
    st.stop()

# -------------------------------
# Data Cleaning
# -------------------------------
df = df.dropna(axis=1, how="all")  # Drop columns that are fully NaN
df = df.dropna(how="all")          # Drop rows that are fully NaN
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]  # Remove unnamed columns

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
    "Missing Values": df.isna().sum()
})
st.dataframe(info, width='stretch')

# -------------------------------
# Filters (Simple Style)
# -------------------------------
st.sidebar.header("🔎 Apply Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        unique_vals = df[col].dropna().unique().tolist()
        if 1 < len(unique_vals) <= 50:
            selected = st.sidebar.multiselect(f"{col}:", unique_vals, default=[])
            if selected:
                filtered_df = filtered_df[filtered_df[col].isin(selected)]

# -------------------------------
# Charts Section
# -------------------------------
st.subheader("📈 Visualization")

chart_type = st.selectbox("Select chart type:", ["None", "Histogram", "Line Chart", "Bar Chart"])
numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()

if chart_type != "None" and not filtered_df.empty and numeric_cols:
    x_axis = st.selectbox("X-axis:", filtered_df.columns)
    y_axis = st.selectbox("Y-axis (numeric):", numeric_cols)

    try:
        if chart_type == "Histogram":
            st.bar_chart(filtered_df[y_axis])
        elif chart_type == "Line Chart":
            st.line_chart(filtered_df.set_index(x_axis)[y_axis])
        elif chart_type == "Bar Chart":
            grouped = filtered_df.groupby(x_axis, dropna=False)[y_axis].sum().reset_index()
            st.bar_chart(grouped.set_index(x_axis))
    except Exception as e:
        st.warning(f"⚠️ Could not render chart: {e}")

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

# -------------------------------
# Gemini Chatbot (Top Right Sidebar)
# -------------------------------
st.sidebar.markdown("---")
st.sidebar.header("🤖 Gemini Chatbot")

if model:
    user_query = st.sidebar.text_area("Ask Gemini about your data:")
    if st.sidebar.button("Ask"):
        if not user_query.strip():
            st.sidebar.warning("Please enter a question.")
        else:
            try:
                response = model.generate_content(f"You are a data analyst. Dataset preview:\n{df.head(10)}\n\nUser Question: {user_query}")
                st.sidebar.success(response.text)
            except Exception as e:
                st.sidebar.error(f"⚠️ Gemini Error: {e}")
else:
    st.sidebar.warning("Gemini API key not configured. Add it to `.env` or Streamlit secrets.")
