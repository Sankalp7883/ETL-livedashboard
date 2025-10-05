import os
import sqlite3
import pandas as pd
import camelot
from transform_generic import transform_dataset

DB_FILE = "datawarehouse.db"
DATA_DIR = "data"

# -----------------------------------------------------
# Utility: ensure the data directory exists
# -----------------------------------------------------
os.makedirs(DATA_DIR, exist_ok=True)

# -----------------------------------------------------
# Save dataframe to SQLite database
# -----------------------------------------------------
def save_to_db(df, table_name):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"[INFO] ✅ Wrote table '{table_name}' with shape {df.shape}")
    except Exception as e:
        print(f"[❌ ERROR] Could not write to DB for '{table_name}': {e}")

# -----------------------------------------------------
# Process an individual file
# -----------------------------------------------------
def process_file(filepath):
    filename = os.path.basename(filepath)
    name, ext = os.path.splitext(filename)
    ext = ext.lower()

    # Skip temporary Excel files
    if filename.startswith("~$"):
        print(f"[SKIP] Temporary Excel file ignored: {filename}")
        return None

    try:
        # --------------- EXCEL ----------------
        if ext in [".xlsx", ".xls"]:
            print(f"[INFO] Reading Excel: {filepath}")
            xls = pd.ExcelFile(filepath)
            frames = []
            for sheet in xls.sheet_names:
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)

                # Find the first row that looks like headers
                first_valid = df_raw.apply(lambda row: row.notna().sum(), axis=1).idxmax()
                df = pd.read_excel(xls, sheet_name=sheet, header=first_valid)

                # Drop unnamed and empty columns
                df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
                df = df.dropna(axis=1, how="all")
                frames.append(df)

            df = pd.concat(frames, ignore_index=True)
            print(f"[INFO] Combined Excel shape: {df.shape}")
            return df

        # --------------- CSV ----------------
        elif ext == ".csv":
            print(f"[INFO] Reading CSV: {filepath}")
            df = pd.read_csv(filepath)
            df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
            df = df.dropna(axis=1, how="all")
            print(f"[INFO] CSV shape: {df.shape}")
            return df

        # --------------- PDF ----------------
        elif ext == ".pdf":
            print(f"[INFO] Reading PDF: {filepath}")
            print(f"[INFO] Trying Camelot first for PDF...")
            tables = camelot.read_pdf(filepath, pages="all")

            if len(tables) > 0:
                dfs = [t.df for t in tables]
                df = pd.concat(dfs, ignore_index=True)
                df = df.replace("", pd.NA).dropna(how="all", axis=1)
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

# -----------------------------------------------------
# Main Load Function
# -----------------------------------------------------
def main():
    print(f"[INFO] Searching for files in: {DATA_DIR}")
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR)
             if os.path.isfile(os.path.join(DATA_DIR, f))]

    print(f"[INFO] Found {len(files)} files: {files}")

    for filepath in files:
        print(f"\n[INFO] Processing {filepath} ...")
        df = process_file(filepath)
        if df is None or df.empty:
            print(f"[WARN] Skipping {filepath}: no valid data")
            continue

        table_name = os.path.splitext(os.path.basename(filepath))[0].lower().replace(" ", "_")
        print(f"[INFO] Transforming dataset for table: {table_name}")
        try:
            df_transformed = transform_dataset(df, table_name)
            if df_transformed is not None and not df_transformed.empty:
                save_to_db(df_transformed, table_name)
            else:
                print(f"[WARN] No data to save for {table_name}")
        except Exception as e:
            print(f"[❌ ERROR] Failed processing {filepath}: {e}")

    print("\n✅ All done! Database updated successfully.")

# -----------------------------------------------------
# Run
# -----------------------------------------------------
if __name__ == "__main__":
    main()
