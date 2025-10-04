import os
import pandas as pd

def extract_files(path):
    """
    Extract tables from CSV, Excel, or PDF into pandas DataFrames.
    Returns:
        - dict of {sheet_or_table_name: DataFrame} if multiple tables
        - single DataFrame if only one table
        - None if nothing found
    """
    ext = os.path.splitext(path)[1].lower()
    results = {}

    try:
        # ---------------------------
        # CSV
        # ---------------------------
        if ext == ".csv":
            try:
                df = pd.read_csv(path)  # default
            except Exception:
                df = pd.read_csv(path, sep=";")  # fallback for ; delimiter

            df = df.dropna(how="all").dropna(axis=1, how="all")
            if not df.empty:
                results["csv"] = df

        # ---------------------------
        # Excel
        # ---------------------------
        elif ext in [".xls", ".xlsx"]:
            excel = pd.ExcelFile(path)
            for sheet in excel.sheet_names:
                df = excel.parse(sheet)
                df = df.dropna(how="all").dropna(axis=1, how="all")
                if not df.empty:
                    results[sheet] = df

        # ---------------------------
        # PDF
        # ---------------------------
        elif ext == ".pdf":
            try:
                import camelot
                tables = camelot.read_pdf(path, pages="all")
                for i, t in enumerate(tables):
                    df = t.df
                    df = df.dropna(how="all").dropna(axis=1, how="all")
                    if not df.empty:
                        results[f"page{i}"] = df
            except Exception:
                try:
                    import pdfplumber
                    with pdfplumber.open(path) as pdf:
                        for i, page in enumerate(pdf.pages):
                            tables = page.extract_tables()
                            for j, table in enumerate(tables):
                                df = pd.DataFrame(table)
                                df = df.dropna(how="all").dropna(axis=1, how="all")
                                if not df.empty:
                                    results[f"page{i}_table{j}"] = df
                except Exception as e:
                    print(f"[WARN] PDF extraction failed for {path}: {e}")

        else:
            print(f"[WARN] Unsupported file type: {path}")

    except Exception as e:
        print(f"[ERROR] Failed extracting {path}: {e}")

    # ---------------------------
    # Return results
    # ---------------------------
    if not results:
        print(f"[WARN] No tabular data found in {os.path.basename(path)}")
        return None

    return results if len(results) > 1 else list(results.values())[0]
