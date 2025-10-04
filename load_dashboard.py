
from extract import extract_data
from transform import transform_data
from pathlib import Path
import sqlite3

# Simple wrapper to run transform and load into DB
data = extract_data('data')  # not used in our tailored flow; keep for compatibility
# We'll read Excel directly in transform for this tailored pipeline
from transform_dashboard import transform_inventory_excel
df_clean = transform_inventory_excel('data/ABC_Book_Stores_Inventory_Register.xlsx')

conn = sqlite3.connect('financial_data.db')
df_clean.to_sql('financial_data', conn, if_exists='replace', index=False)
conn.close()

print('Loaded cleaned data into financial_data.db')
