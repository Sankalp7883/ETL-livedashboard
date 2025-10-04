
import pandas as pd
import re

def _clean_colname(col):
    c = str(col).strip()
    c = c.replace('\\ufeff', '')  # remove BOM if present
    c = c.lower()
    c = re.sub(r'[^0-9a-z]+', '_', c)
    c = c.strip('_')
    return c

def standardize_columns(df):
    """Normalize column names and map common aliases to canonical names."""
    df = df.copy()
    # Basic normalization
    rename_map = {col: _clean_colname(col) for col in df.columns}
    df = df.rename(columns=rename_map)
    # Alias groups -> canonical name
    alias_map = {
        'opening_stock': ['opening_stock','opening','open_stock','begin_stock','beginning_stock','openingstock'],
        'closing_stock': ['closing_stock','closing','end_stock','ending_stock','closingstock','ending_stock'],
        'purchases': ['purchases','purchase','procurement','buy'],
        'sales': ['sales','sale','sold'],
        'revenue': ['revenue','income','total_revenue','amount','total'],
        'date': ['date','transaction_date','date_of_transaction','dt','timestamp']
    }
    # For each column, attempt to map to a canonical name based on alias substrings
    rename_candidates = {}
    cols = list(df.columns)
    for col in cols:
        # exact match to canonical
        if col in alias_map:
            continue
        mapped = None
        for canonical, aliases in alias_map.items():
            for a in aliases:
                if a == col or a in col:
                    mapped = canonical
                    break
            if mapped:
                break
        if mapped and col != mapped:
            # Avoid overwriting an existing canonical column accidentally
            if mapped not in df.columns:
                rename_candidates[col] = mapped
    if rename_candidates:
        df = df.rename(columns=rename_candidates)
    return df

def transform_data(dataframes):
    """
    Cleans and combines a list of DataFrames from extract.py.
    - Normalizes column names and maps common aliases.
    - Converts numeric columns to numbers.
    - Computes closing_stock when possible.
    """
    cleaned_frames = []

    for df in dataframes:
        if not isinstance(df, pd.DataFrame):
            continue
        if df.empty:
            continue

        # Normalize and map column names
        df = standardize_columns(df)

        # If the DataFrame looks like a PDF extraction with only 'text', skip it
        if set(df.columns) <= {'text', 'source_file'}:
            continue

        # Ensure numeric columns exist and are numeric
        for col in ['opening_stock', 'purchases', 'sales', 'closing_stock', 'revenue']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Calculate closing_stock if possible and not present
        if 'closing_stock' not in df.columns and {'opening_stock', 'purchases', 'sales'}.issubset(df.columns):
            df['closing_stock'] = df['opening_stock'] + df['purchases'] - df['sales']

        # Drop fully empty rows, drop duplicates
        df = df.drop_duplicates().dropna(how='all')

        cleaned_frames.append(df)

    # Combine all DataFrames
    if cleaned_frames:
        combined_df = pd.concat(cleaned_frames, ignore_index=True, sort=False)
        # Try parsing date if present
        if 'date' in combined_df.columns:
            combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
    else:
        combined_df = pd.DataFrame()

    return combined_df


if __name__ == "__main__":
    from extract import extract_data
    raw = extract_data("data")
    cleaned = transform_data(raw)
    print("Preview of cleaned data:")
    print(cleaned.head())
