# Ecommerce Sales Data Processing

This project provides a robust framework for processing, analyzing, and validating e-commerce sales data. It is designed for data engineers and analysts who need to ingest, clean, enrich, and check the quality of sales data from multiple sources.

## Features

- **Raw Data Ingestion:**
  - Load customer, order, and product data from various formats (Excel, JSON, CSV).
- **Data Quality Checks:**
  - Automated validation and quality checks to ensure data integrity.
- **Data Enrichment:**
  - Create enriched tables by joining and transforming raw data for analytics.
- **Configurable Pipeline:**
  - Centralized configuration via YAML for flexible pipeline management.
- **Testing Suite:**
  - Pytest-based tests for configuration, data quality, and table creation.
- **Jupyter Notebook:**
  - Example notebook for exploratory data analysis and reporting.

## Project Structure

```
Ecommerce-Sales-Data-Processing/
├── configs/
│   └── config.yaml           # Pipeline configuration
├── data/
│   ├── Customer.xlsx         # Customer data (Excel)
│   ├── Orders.json           # Orders data (JSON)
│   └── Products.csv          # Products data (CSV)
├── notebook/
│   └── e_commerce_sales_data_analysis.ipynb  # EDA notebook
├── pytest/
│   ├── pytest_run.py.ipynb   # Test runner notebook
│   └── tests/                # Pytest test cases
├── src/
│   ├── config.py             # Config loader
│   ├── create_raw_tables.py  # Raw data ingestion
│   ├── create_enriched_tables.py # Data enrichment
│   └── data_quality_checks.py    # Data validation
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

## Getting Started

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the Pipeline

Edit `configs/config.yaml` to set file paths and processing options.

### 3. Run Data Processing

Run the scripts in `src/` to ingest, validate, and enrich your data:

```bash
python src/create_raw_tables.py
python src/data_quality_checks.py
python src/create_enriched_tables.py
```

### 4. Explore Data

Open the Jupyter notebook for analysis:

```bash
jupyter notebook notebook/e_commerce_sales_data_analysis.ipynb
```

### 5. Run Tests

Run all tests using pytest:

```bash
pytest pytest/tests/
```

Or use the test runner notebook:

```bash
jupyter notebook pytest/pytest_run.py.ipynb
```

## Requirements

- Python 3.8+
- See `requirements.txt` for package dependencies

## License

MIT License