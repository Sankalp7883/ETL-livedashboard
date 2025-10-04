# extract.py
from pathlib import Path
import pandas as pd
import pdfplumber

def extract_data(folder_path):
    """
    Reads all supported files (CSV, Excel, PDF) from the given folder
    and returns a list of DataFrames.
    """
    all_data = []
    folder = Path(folder_path)

    for file in folder.glob('*'):
        if file.suffix.lower() == '.csv':
            print(f"Reading CSV: {file.name}")
            df = pd.read_csv(file)
            df['source_file'] = file.name
            all_data.append(df)

        elif file.suffix.lower() in ['.xls', '.xlsx']:
            print(f"Reading Excel: {file.name}")
            df = pd.read_excel(file)
            df['source_file'] = file.name
            all_data.append(df)

        elif file.suffix.lower() == '.pdf':
            print(f"Reading PDF: {file.name}")
            text_data = []
            with pdfplumber.open(file) as pdf:
                for page in pdf.pages:
                    text_data.append(page.extract_text())
            df = pd.DataFrame({'text': text_data, 'source_file': file.name})
            all_data.append(df)

        else:
            print(f"Skipping unsupported file type: {file.name}")

    return all_data

if __name__ == "__main__":
    folder_path = "data"
    dataframes = extract_data(folder_path)
    print(f"\nExtracted {len(dataframes)} files successfully!")

