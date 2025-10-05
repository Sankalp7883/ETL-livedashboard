import os
import pandas as pd
import sqlite3
import camelot
from transform_generic import transform_dataset

DB_FILE = "datawarehouse.db"
DATA_DIR = "data"

# -------------------------------
# Function: Load file into DB
# -------------------------------
def load_to_db(df, table_name):
    with sqlite3.connect(DB_FILE) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"[INFO] ✅ Wrote table '{table_name}' with shape {df.shape}")

# -------------------------------
# Function: Extract data from files
# -------------------------------
def process_file(filepath):
    filename = os.path.basename(filepath)
    name, ext = os.path.splitext(filename)
    ext = ext.lower()

    if filename.startswith("~$"):
        print(f"[SKIP] Temporary Excel file ignored: {filename}")
        return None

    try:
        if ext in [".xlsx", ".xls"]:
            print(f"[INFO] Reading Excel: {filepath}")
            xls = pd.ExcelFile(filepath)
            frames = []
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                frames.append(df)
            df = pd.concat(frames, ignore_index=True)
            print(f"[INFO] Combined Excel shape: {df.shape}")
            return df

        elif ext == ".csv":
            print(f"[INFO] Reading CSV: {filepath}")
            df = pd.read_csv(filepath)
            print(f"[INFO] CSV shape: {df.shape}")
            return df

        elif ext == ".pdf":
            print(f"[INFO] Reading PDF: {filepath}")
            print(f"[INFO] Trying Camelot first for PDF...")
            tables = camelot.read_pdf(filepath, pages="all")
            if tables:
                dfs = [t.df for t in tables]
                df = pd.concat(dfs, ignore_index=True)
                print(f"[INFO] ✅ Extracted {len(tables)} tables using Camelot. Shape={df.shape}")
                return df
            else:
                print(f"[WARN] No tables found in PDF.")
                return None

        else:
            print(f"[WARN] Unsupported file type: {ext}")
            return None

    except Exception as e:
        print(f"[❌ ERROR] Failed processing {filepath}: {e}")
        return None

# -------------------------------
# Main ETL Process
# -------------------------------
if __name__ == "__main__":
    print(f"[INFO] Searching for files in {DATA_DIR}...")
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR)]
    print(f"[INFO] Found {len(files)} files: {files}")

    for f in files:
        print(f"\n[INFO] Processing {f} ...")
        df = process_file(f)
        if df is None or df.empty:
            continue

        try:
            table_name = os.path.splitext(os.path.basename(f))[0].lower().replace(" ", "_")
            df = transform_dataset(df, table_name)
            load_to_db(df, table_name)
        except Exception as e:
            print(f"[❌ ERROR] Failed processing {f}: {e}")

    print("\n✅ All done! Database updated successfully.")
