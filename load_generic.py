import os
import sqlite3
import pandas as pd
from extract_generic import extract_files as extract_file   # FIXED here
from transform_generic import transform_generic

DATA_DIR = "data"
DB_FILE = "datawarehouse.db"

def save_to_sqlite(df, table_name, conn):
    df = df.copy()
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"[INFO] Wrote table {table_name}, shape={df.shape}")

def main():
    conn = sqlite3.connect(DB_FILE)
    for fname in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, fname)
        try:
            data = extract_file(path)   # still works
        except Exception as e:
            print(f"[ERROR] Skipping {fname}: {e}")
            continue

        if isinstance(data, dict):  # multi-sheet or multi-table
            for subname, df in data.items():
                if df is None or df.empty:
                    continue
                df, types = transform_generic(df, table_name=subname)
                table_name = f"t_data_{os.path.basename(fname).replace('.','_')}__{subname}"
                save_to_sqlite(df, table_name, conn)
        elif isinstance(data, pd.DataFrame):
            df, types = transform_generic(data, table_name=fname)
            table_name = f"t_data_{os.path.basename(fname).replace('.','_')}"
            save_to_sqlite(df, table_name, conn)
        else:
            print(f"[WARN] No tabular data found in {fname}")

    conn.close()

if __name__ == "__main__":
    main()
