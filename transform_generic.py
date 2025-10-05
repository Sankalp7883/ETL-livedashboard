import pandas as pd
import numpy as np
import sqlite3
import os

DB_FILE = "datawarehouse.db"

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe by removing unnecessary columns/rows and fixing headers."""
    if df is None or df.empty:
        return df

    # Remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # Drop columns that are entirely NaN
    df = df.dropna(axis=1, how="all")

    # Drop rows that are entirely NaN
    df = df.dropna(axis=0, how="all")

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Remove duplicate columns (if any)
    df = df.loc[:, ~df.columns.duplicated()]

    # Try to parse datetime columns automatically
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                if parsed.notna().sum() > len(df) * 0.6:
                    df[col] = parsed
            except Exception:
                pass

    return df


def transform_dataset(df: pd.DataFrame, output_table: str):
    """Transform and save cleaned data to SQLite database."""
    df = clean_dataframe(df)
    if df is None or df.empty:
        print(f"[WARN] Skipping {output_table} — no valid data after cleaning.")
        return

    with sqlite3.connect(DB_FILE) as conn:
        df.to_sql(output_table, conn, if_exists="replace", index=False)
        print(f"[INFO] Wrote table {output_table}, shape={df.shape}")


def transform_and_save(input_data, table_name="generic_table"):
    """Unified entry point — accepts DataFrame or list of DataFrames."""
    if isinstance(input_data, list):
        combined = pd.concat([clean_dataframe(d) for d in input_data if not d.empty], ignore_index=True)
        transform_dataset(combined, table_name)
    elif isinstance(input_data, pd.DataFrame):
        transform_dataset(input_data, table_name)
    else:
        print(f"[ERROR] Invalid data type for {table_name}: {type(input_data)}")
