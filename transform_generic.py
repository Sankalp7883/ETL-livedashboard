import pandas as pd
import numpy as np

def transform_dataset(df, output_table):
    """
    Generic data transformation function for ETL dashboard.
    --------------------------------------------------------
    1️⃣ Cleans column names
    2️⃣ Handles duplicates and missing values
    3️⃣ Automatically detects data types (datetime, numeric, text)
    4️⃣ Converts timestamps like 1714176000000000000 → real dates
    5️⃣ Drops empty rows
    """

    print(f"[INFO] Transforming dataset for table: {output_table}")

    if df is None or df.empty:
        print(f"[WARN] Empty DataFrame received for {output_table}")
        return df

    # ---------------------------------------------------------------------
    # 🧹 STEP 1 — Clean column names
    # ---------------------------------------------------------------------
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(r"[^\w\s]", "_", regex=True)  # replace special chars
        .str.replace("__+", "_", regex=True)       # avoid double underscores
        .str.lower()
    )

    # ---------------------------------------------------------------------
    # 🩺 STEP 2 — Handle duplicates, empties
    # ---------------------------------------------------------------------
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.replace(["", "NA", "NaN", "nan", "null", "NULL"], np.nan)

    # ---------------------------------------------------------------------
    # 🔍 STEP 3 — Detect and fix data types
    # ---------------------------------------------------------------------
    for col in df.columns:
        s = df[col]

        # --- Date detection based on column name ---
        if any(word in col for word in ["date", "time", "timestamp"]):
            try:
                df[col] = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
                print(f"[INFO] Column '{col}': converted to datetime (by name)")
                continue
            except Exception:
                pass

        # --- Try to detect datetime based on values ---
        if s.dropna().astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").any():
            try:
                df[col] = pd.to_datetime(s, errors="coerce")
                print(f"[INFO] Column '{col}': converted to datetime (by value pattern)")
                continue
            except Exception:
                pass

        # --- Handle numeric timestamps ---
        if pd.api.types.is_numeric_dtype(s):
            try:
                max_val = s.dropna().max()
                if max_val > 1e14:  # nanoseconds
                    df[col] = pd.to_datetime(s, errors="coerce", unit="ns")
                    print(f"[INFO] Column '{col}': converted epoch ns → datetime")
                elif max_val > 1e10:  # milliseconds
                    df[col] = pd.to_datetime(s, errors="coerce", unit="ms")
                    print(f"[INFO] Column '{col}': converted epoch ms → datetime")
                else:
                    df[col] = pd.to_numeric(s, errors="coerce")
                    print(f"[INFO] Column '{col}': numeric")
                continue
            except Exception:
                pass

        # --- Otherwise treat as categorical/text ---
        df[col] = s.astype(str).str.strip()
        print(f"[INFO] Column '{col}': categorical")

    # ---------------------------------------------------------------------
    # 🧽 STEP 4 — Drop fully empty rows
    # ---------------------------------------------------------------------
    df = df.dropna(how="all")

    # ---------------------------------------------------------------------
    # 📊 STEP 5 — Summary
    # ---------------------------------------------------------------------
    print(f"[INFO] ✅ Transformed table '{output_table}', shape={df.shape}")

    return df
