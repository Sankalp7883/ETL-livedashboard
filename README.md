# ETL-LiveDashboard 🚀  

A flexible **ETL (Extract, Transform, Load) pipeline** with an interactive **Streamlit dashboard** for real-time data exploration.  
This project can handle multiple data sources like **Excel, CSV, and PDF** files, automatically transform them, and visualize insights dynamically.  

---

## 🔹 Features
- **Flexible ETL pipeline**  
  - Extracts tabular data from Excel, CSV, and PDF.  
  - Cleans and transforms data automatically (detects numeric, categorical, datetime columns).  
  - Stores results in SQLite for easy querying.  

- **Streamlit dashboards**  
  - Upload datasets and explore them in real time.  
  - Automatic data profiling (missing values, column types, etc.).  
  - Interactive charts (bar, line, pie, top-10 analysis).  
  - Filters by category, date range, or column selection.  

- **Smart transformations**  
  - Auto-detects dates, amounts, categories.  
  - Currency formatting (₹ Lakhs, Millions, etc.).  
  - Drilldown visualizations per dataset.  

- **Downloadable results**  
  - Export cleaned data as **CSV/Excel**.  
  - Query processed datasets directly.  

---

## 📂 Project Structure

hackathon_etl/
│
├── app_generic.py               # Streamlit dashboard
├── extract_generic.py           # Extracts data from files
├── transform_generic.py         # Cleans & transforms datasets
├── load_generic.py              # Combines Extract + Transform + Load
│
├── data/                        # Put your Excel, PDF, CSV files here
│   ├── ABC_Book_Stores_Inventory_Register.xlsx
│   ├── dataset_pdf.pdf
│   └── financial_sample.csv
│
├── datawarehouse.db             # Generated SQLite database
├── requirements.txt             # Dependencies list
└── README.md                    # Project documentation

