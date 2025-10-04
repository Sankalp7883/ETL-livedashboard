# load.py
import sqlite3
from transform import transform_data
from extract import extract_data

def load_to_db(df, db_path='financial_data.db'):
    """
    Saves the cleaned DataFrame into SQLite database.
    """
    with sqlite3.connect(db_path) as conn:
        # Replace table if it exists
        df.to_sql('financial_data', conn, if_exists='replace', index=False)
    print(f"Data loaded into {db_path} successfully!")

if __name__ == "__main__":
    # Extract and transform first
    raw_data = extract_data("data")
    cleaned_data = transform_data(raw_data)

    # Load into database
    load_to_db(cleaned_data)
