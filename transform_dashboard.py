
import pandas as pd

def transform_inventory_excel(path):
    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, sheet_name='Inventory Register')
    # Normalize columns by stripping
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {
        'Date of Sales Invoice': 'date',
        'Issued from the Opening Stock': 'opening_stock',
        'Issued from the Current year': 'purchases',
        'Total amount': 'revenue',
        'Closing Stock No. of Units': 'closing_stock_units',
        'Closing Stock Total amount': 'closing_stock_value',
    }
    rename_map = {k: v for k, v in mapping.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    # Convert numeric columns
    for col in ['opening_stock','purchases','revenue','closing_stock_units','closing_stock_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # Parse date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Keep only relevant columns
    keep = [c for c in ['date','opening_stock','purchases','revenue','closing_stock_units','closing_stock_value'] if c in df.columns]
    return df[keep]
