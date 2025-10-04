
# lightweight extract placeholder to keep compatibility
from pathlib import Path
import pandas as pd

def extract_data(folder_path):
    folder = Path(folder_path)
    xls = folder / 'ABC_Book_Stores_Inventory_Register.xlsx'
    if xls.exists():
        df = pd.read_excel(xls, sheet_name='Inventory Register')
        return [df]
    return []
