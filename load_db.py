import sqlite3
import pandas as pd

# Connect to (or create) the database
conn = sqlite3.connect("financial_data.db")

# Read CSV
df = pd.read_csv("data/financial_sample.csv")

# Load into SQLite table
df.to_sql("financial_data", conn, if_exists="replace", index=False)

# Query the table
result = pd.read_sql_query("SELECT * FROM financial_data;", conn)
print(result)

conn.close()
