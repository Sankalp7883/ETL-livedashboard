import os
import sqlite3
import pandas as pd
from extract_generic import extract_files
from transform_generic import transform_dataset

# -------------------------------
# Database file and data folder
# -------------------------------
DB_FILE = "datawarehouse.db"
DATA_DIR = "data"

# -------------------------------
# Save a DataFrame to SQLite
# -------------------------------
def load_to_db(df, table_name):
    """Write transformed dataframe to SQLite database"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"[INFO] ✅ Wrote table '{table_name}' with shape {df.shape}")
    except Exception as e:
        print(f"[ERROR] ❌ Failed to write table {table_name}: {e}")

# -------------------------------
# Main ETL Process
# -------------------------------
def main():
    # Find all files in /data folder
    if not os.path.exists(DATA_DIR):
        print(f"[ERROR] ❌ Data folder not found: {DATA_DIR}")
        return

    files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if not f.startswith(".") and not os.path.isdir(os.path.join(DATA_DIR, f))
    ]

    print(f"[INFO] Found {len(files)} files: {files}")
    if not files:
        print("[WARN] ⚠️ No files found in the data directory.")
        return

    for file_path in files:
        try:
            print(f"\n[INFO] Processing {file_path} ...")
            df = extract_files(file_path)

            # Some extractors may return list of DataFrames (Excel/PDF)
            if isinstance(df, list):
                df = pd.concat(df, ignore_index=True)

            if df is None or df.empty:
                print(f"[WARN] No data extracted from {file_path}")
                continue

            # Create table name based on file name
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            table_name = base_name.lower().replace(" ", "_")

            # 🧠 FIX: pass output_table argument to transform_dataset
            df_transformed = transform_dataset(df, table_name)

            # Load to SQLite
            load_to_db(df_transformed, table_name)

        except Exception as e:
            print(f"[❌ ERROR] Failed processing {file_path}: {e}")

    print("\n✅ All done! Database updated successfully.")

# -------------------------------
# Run the script
# -------------------------------
if __name__ == "__main__":
    main()
