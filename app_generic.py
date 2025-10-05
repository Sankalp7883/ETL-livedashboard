import streamlit as st
import pandas as pd
import sqlite3
import os
from google.genai import genai
from dotenv import load_dotenv
import io

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    st.error("❌ Missing Gemini API key! Add it in a `.env` file like:\nGEMINI_API_KEY=your_key_here")
    st.stop()

# Initialize Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL = "models/gemini-2.5-flash"

# -------------------------------
# Database helpers
# -------------------------------
DB_FILE = "datawarehouse.db"

def get_table_names():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [r[0] for r in cursor.fetchall()]
    except Exception:
        return []

def load_table(name):
    with sqlite3.connect(DB_FILE) as conn:
        return pd.read_sql_query(f"SELECT * FROM '{name}'", conn)

# -------------------------------
# Streamlit layout
# -------------------------------
st.set_page_config(page_title="ETL Dashboard + Gemini Chatbot", layout="wide")
st.title("📊 ETL Live Dashboard + 🤖 Gemini Chat Assistant")

# --- Sidebar: Chatbot ---
st.sidebar.header("💬 Gemini Chatbot")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

prompt = st.sidebar.text_area("Ask Gemini about your data:")
if st.sidebar.button("Ask Gemini"):
    try:
        context = ""
        # If user already loaded a dataframe, include a data summary
        if "df" in st.session_state and st.session_state.df is not None:
            df = st.session_state.df
            csv_buf = io.StringIO()
            df.head(100).to_csv(csv_buf, index=False)
            context = f"Dataset preview (first 100 rows):\n{csv_buf.getvalue()}\n\n"
        else:
            context = "No dataset currently loaded.\n"

        full_prompt = (
            f"You are a data analyst. Use the dataset below to answer the question.\n\n"
            f"{context}\n\nUser Question: {prompt}\n"
        )

        response = client.models.generate_content(
            model=MODEL,
            contents=full_prompt
        )

        reply = response.text if hasattr(response, "text") else "⚠️ No response received."
        st.session_state.chat_history.append(("You", prompt))
        st.session_state.chat_history.append(("Gemini", reply))
    except Exception as e:
        st.sidebar.error(f"⚠️ Gemini Error: {e}")

# Display chat history
for sender, msg in st.session_state.chat_history[-6:]:
    st.sidebar.markdown(f"**{sender}:** {msg}")

st.sidebar.divider()

# --- Sidebar: Data Source ---
st.sidebar.header("📁 Data Source")
source_option = st.sidebar.radio("Choose data source:", ["Existing DB Table", "Upload File"])

df = None
if source_option == "Existing DB Table":
    tables = get_table_names()
    if not tables:
        st.error("⚠️ No tables found in the database. Run `load_generic.py` first.")
        st.stop()
    table_choice = st.sidebar.selectbox("Select a table:", tables)
    df = load_table(table_choice)
    st.success(f"✅ Loaded table: `{table_choice}`")

elif source_option == "Upload File":
    uploaded = st.sidebar.file_uploader("📤 Upload Excel/CSV file", type=["xlsx", "csv"])
    if uploaded:
        try:
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded)
            elif uploaded.name.endswith(".csv"):
                df = pd.read_csv(uploaded)
        except Exception as e:
            st.error(f"Error reading file: {e}")

if df is None:
    st.info("👈 Upload a file or choose an existing table to start.")
    st.stop()

st.session_state.df = df  # Save dataset for chatbot context

# -------------------------------
# Data Preview
# -------------------------------
st.subheader("🔍 Data Preview")
st.dataframe(df.head(50), width="stretch")

# -------------------------------
# Column Info
# -------------------------------
st.subheader("📋 Column Summary")
info = pd.DataFrame({
    "Column": df.columns,
    "Data Type": df.dtypes.astype(str),
    "Missing Values": df.isna().sum(),
})
st.dataframe(info, width="stretch")

# -------------------------------
# Filters
# -------------------------------
st.sidebar.header("🔎 Filters")
filtered_df = df.copy()

for col in df.columns:
    if df[col].dtype == "object":
        values = df[col].dropna().unique().tolist()
        if 1 < len(values) <= 50:
            selected = st.sidebar.multiselect(f"{col}:", values)
            if selected:
                filtered_df = filtered_df[filtered_df[col].isin(selected)]

# -------------------------------
# Charts
# -------------------------------
st.subheader("📈 Data Visualization")
numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()

chart_type = st.selectbox("Select chart type:", ["None", "Bar", "Line", "Histogram"])
if chart_type != "None" and numeric_cols:
    x_axis = st.selectbox("X-axis:", df.columns)
    y_axis = st.selectbox("Y-axis (numeric):", numeric_cols)
    if chart_type == "Bar":
        grouped = filtered_df.groupby(x_axis, dropna=False)[y_axis].sum().reset_index()
        st.bar_chart(grouped.set_index(x_axis))
    elif chart_type == "Line":
        st.line_chart(filtered_df.set_index(x_axis)[y_axis])
    elif chart_type == "Histogram":
        st.bar_chart(filtered_df[y_axis])

# -------------------------------
# Download Filtered Data
# -------------------------------
st.subheader("💾 Download Filtered Data")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Download CSV", csv, "filtered_data.csv", "text/csv")
