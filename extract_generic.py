import os
import pandas as pd
import camelot
import pdfplumber

# ---------------------------------------
# Universal File Extractor
# ---------------------------------------
def extract_files(file_path):
    """
    Smart file extractor for PDF, Excel, and CSV.
    - Excel: all sheets merged
    - CSV: read directly
    - PDF: hybrid extraction (Camelot + pdfplumber fallback)
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext in [".xlsx", ".xls"]:
        print(f"[INFO] Reading Excel: {file_path}")
        excel = pd.ExcelFile(file_path)
        dfs = [excel.parse(sheet) for sheet in excel.sheet_names]
        df = pd.concat(dfs, ignore_index=True)
        print(f"[INFO] Combined Excel shape: {df.shape}")
        return df

    elif ext == ".csv":
        print(f"[INFO] Reading CSV: {file_path}")
        df = pd.read_csv(file_path)
        print(f"[INFO] CSV shape: {df.shape}")
        return df

    elif ext == ".pdf":
        print(f"[INFO] Reading PDF: {file_path}")
        return extract_pdf_hybrid(file_path)

    else:
        print(f"[WARN] Unsupported file format: {ext}")
        return pd.DataFrame()


# ---------------------------------------
# Smart Hybrid PDF Extraction
# ---------------------------------------
def extract_pdf_hybrid(file_path):
    """
    Try to extract tables from PDF with Camelot.
    If no tables found or parsing fails → fallback to text mode (pdfplumber).
    """
    try:
        print(f"[INFO] Trying Camelot first for PDF...")
        tables = camelot.read_pdf(file_path, pages="all", flavor="lattice")

        if tables and len(tables) > 0:
            dfs = [t.df for t in tables if not t.df.empty]
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
                print(f"[INFO] ✅ Extracted {len(dfs)} tables using Camelot. Shape={df.shape}")
                return df

        print(f"[WARN] No tables found via Camelot. Trying text mode...")
        return extract_pdf_textmode(file_path)

    except Exception as e:
        print(f"[WARN] Camelot failed: {e}. Falling back to text mode...")
        return extract_pdf_textmode(file_path)


# ---------------------------------------
# Text Mode PDF Parsing (Fallback)
# ---------------------------------------
def extract_pdf_textmode(file_path):
    """
    Fallback parser for PDFs that don’t have clear tables.
    Extracts all lines, splits on consistent whitespace, and guesses columns.
    """
    try:
        all_lines = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    lines = [line.strip() for line in text.split("\n") if line.strip()]
                    all_lines.extend(lines)

        # Try to split by multiple spaces to form columns
        split_lines = [line.split() for line in all_lines if " " in line]
        max_cols = max(len(row) for row in split_lines)
        df = pd.DataFrame(split_lines, columns=[f"col_{i+1}" for i in range(max_cols)])
        print(f"[INFO] ✅ Parsed text-based PDF with shape {df.shape}")
        return df

    except Exception as e:
        print(f"[ERROR] ❌ Failed text-based PDF extraction: {e}")
        return pd.DataFrame()
