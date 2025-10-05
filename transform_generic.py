import pandas as pd
import numpy as np

def transform_dataset(df: pd.DataFrame, output_table: str):
    """
    Cleans and standardizes the dataset before loading into the database.
    """

    print(f"[INFO] Transforming dataset for table: {output_table}")

    # 1️⃣ Remove unnamed and empty columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.dropna(axis=1, how='all')

    # 2️⃣ Drop completely empty rows
    df = df.dropna(how='all')

    # 3️⃣ Clean column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^A-Za-z0-9_]+", "", regex=True)
    )

    # 4️⃣ Try converting numeric date columns to readable dates
    for col in df.columns:
        if df[col].dtype in [np.int64, np.float64]:
            if df[col].between(1e12, 2e12).any():  # typical timestamp in ns
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', unit='ns')
                    print(f"[INFO] Converted {col} from timestamp → datetime")
                except Exception:
                    pass

    # 5️⃣ Detect potential date columns
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 6️⃣ Replace inf with NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # 7️⃣ Strip text columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    print(f"[INFO] Transformed table {output_table}, shape={df.shape}")
    return df
