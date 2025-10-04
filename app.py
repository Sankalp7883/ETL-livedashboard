
import streamlit as st
import pandas as pd
import sqlite3
import os
import traceback

st.set_page_config(page_title="Financial Dashboard", layout="wide")
st.title("💰 Financial Dashboard")

DB_PATH = "financial_data.db"

def safe_read_db(db_path):
    if not os.path.exists(db_path):
        st.error(f"Database not found at: {db_path}")
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM financial_data", conn)
        conn.close()
        return df
    except Exception as e:
        st.error("Failed to read database.")
        st.exception(e)
        return pd.DataFrame()

df = safe_read_db(DB_PATH)

st.markdown("**Loaded columns:**")
st.write(df.columns.tolist())

# --- KPIs --
st.header("Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Transactions", len(df))
col2.metric("Total Opening Stock", df['opening_stock'].sum() if 'opening_stock' in df.columns else 0)
col3.metric("Total Closing Stock", df['closing_stock'].sum() if 'closing_stock' in df.columns else 0)
col4.metric("Total Revenue", df['revenue'].sum() if 'revenue' in df.columns else 0)

# --- Trend Charts ---
st.header("Trends Over Time")

if df.empty:
    st.info("No data available. Please run your ETL (load.py or load_db.py) to populate financial_data.db.")
else:
    # Ensure date parsing if present
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Stock trend: handle missing columns gracefully
        stock_cols = [c for c in ['opening_stock', 'closing_stock'] if c in df.columns]
        if stock_cols:
            try:
                trend_stock = df.groupby('date')[stock_cols].sum().sort_index()
                st.subheader("Stock Trend")
                st.line_chart(trend_stock)
            except Exception as e:
                st.error("Could not generate stock trend chart.")
                st.exception(e)
        else:
            st.info("No stock columns (opening_stock / closing_stock) found — skipping Stock Trend.")

        # Revenue trend
        if 'revenue' in df.columns:
            try:
                trend_revenue = df.groupby('date')['revenue'].sum().sort_index()
                st.subheader("Revenue Trend")
                st.line_chart(trend_revenue)
            except Exception as e:
                st.error("Could not generate revenue trend chart.")
                st.exception(e)
        else:
            st.info("No 'revenue' column found — skipping Revenue Trend.")
    else:
        st.info("No 'date' column present; trends require a date field.")

# --- Raw Data Preview ---
st.header("Data Preview")
st.dataframe(df.head(50))

# --- Helpful debug snippets ---
with st.expander("Debug / Fix tips"):
    st.markdown("""
- If you see columns with unexpected names (e.g. `opening stock`, `OpeningStock`, `Opening`), consider standardizing them in your source files or in `transform.py`.
- To re-run the ETL that loads the database, run in your project folder:
  - `python load.py`  (will run extract -> transform -> load)
  - or `python load_db.py` (quick load from `data/financial_sample.csv` if present)
- To inspect what is actually stored in the database from a Python REPL:
```python
import sqlite3, pandas as pd
conn = sqlite3.connect("financial_data.db")
df = pd.read_sql("SELECT * FROM financial_data LIMIT 5;", conn)
print(df.columns.tolist())
print(df.head())
conn.close()
```
- If charts look empty, check the "Loaded columns" list above for column names and ensure there is a valid `date` column.
""")
