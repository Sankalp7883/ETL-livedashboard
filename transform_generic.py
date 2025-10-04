import pandas as pd
import numpy as np
import re
import warnings

# Suppress only pandas datetime "infer format" spam
warnings.filterwarnings("ignore", message="Could not infer format", category=UserWarning)

def _clean_colname(c, idx=None):
    """Clean column names: lowercase, replace spaces with underscores, remove special chars."""
    if not isinstance(c, str):
        c = str(c) if c is not None else f"col{idx}"
    c = c.strip().lower()
    c = re.sub(r"[^\w\s]", "", c)
    c = re.sub(r"\s+", "_", c)
    if c == "":
        c = f"col{idx}"
    return c

def infer_types(df):
    """Try to infer column types: numeric, datetime, categorical."""
    types = {}
    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            types[col] = "unknown"
            continue

        # Numeric?
        try:
            pd.to_numeric(s, errors="raise")
            types[col] = "numeric"
            print(f"[INFO] Column {col}: numeric")
            continue
        except Exception:
            pass

        # Datetime?
        parsed = pd.to_datetime(s, errors="coerce")
        if parsed.notna().sum() > 0.6 * len(s):
            types[col] = "datetime"
            print(f"[INFO] Column {col}: datetime")
            continue

        # Otherwise categorical
        types[col] = "categorical"
        print(f"[INFO] Column {col}: categorical")

    return types

def transform_generic(df, table_name=None):
    """Clean up dataframe: rename columns, coerce types, drop duplicates."""
    df = df.copy()

    # Clean column names
    df.columns = [_clean_colname(c, i) for i, c in enumerate(df.columns)]

    # Drop empty columns
    df = df.dropna(axis=1, how="all")

    # Infer and apply types
    col_types = infer_types(df)
    for col, typ in col_types.items():
        if typ == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif typ == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = df.drop_duplicates()

    print(f"[INFO] Transformed table {table_name}, shape={df.shape}")
    return df, col_types
