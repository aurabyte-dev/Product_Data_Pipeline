# Product Data Pipeline


ETL pipeline for product data analysis - ingests CSV data, validates data quality, flags problematic records, and generates analytical reports.


## Project Overview

This project implements a complete data engineering pipeline that processes product data through multiple stages: ingestion, cleaning, validation, rejection, and analytics generation. The pipeline identifies data quality issues, separates valid products from problematic ones, and generates summary statistics.

## Pipeline Architecture

### Data Pipeline Stages
```
INGEST ─▶ STORAGE ─▶ TRANSFORM ─▶ ACCESS
```

**1. INGEST**
- Read raw product data from CSV files
- Initial data loading into memory
- Technology: `pandas.read_csv()`

**2. STORAGE**
- Temporary storage in DataFrames (in-memory)
- Raw data preserved in `data/raw/`
- Processed outputs saved to `data/processed/`

**3. TRANSFORM**
- Data cleaning (whitespace removal, case normalization)
- Type conversion (strings → numeric, datetime)
- Data quality flagging (missing values, outliers, duplicates)
- Rejection logic (separate valid from invalid products)

**4. ACCESS**
- Export cleaned, validated, and analyzed data as CSV files
- Generate analytical reports and summaries
- Make data available for downstream consumption

---

## Technologies Used

### Core Libraries

| Technology | Purpose | Usage in Project |
|------------|---------|------------------|
| **Pandas** | Data manipulation and analysis | DataFrame operations, CSV I/O, statistical calculations |
| **Python** | Programming language | ETL pipeline orchestration, data transformations |

### Bonus Technologies (Optional)
- **Pydantic**: Data validation using Python type hints (schema enforcement)
- **Psycopg3**: PostgreSQL database adapter (for database integration instead of CSV)

---

## ETL Process Explained

### Extract → Transform → Load

**EXTRACT**
- **What**: Read data from source (CSV file)
- **How**: `pd.read_csv()` ingests raw product data
- **Output**: Raw DataFrame with unprocessed data
```python
raw_df = pd.read_csv("data/raw/products_raw.csv", sep=';')
```

**TRANSFORM**
- **What**: Clean, validate, and enrich data
- **How**: Apply transformations step-by-step:
  1. **Clean**: Remove whitespace, normalize case, standardize formats
  2. **Convert**: Transform data types (text → numbers, dates)
  3. **Flag**: Identify data quality issues (missing values, outliers, duplicates)
  4. **Reject**: Separate invalid products from valid ones
```python
# Cleaning
clean_df = clean_whitespace(raw_df)
clean_df = case_normalization(clean_df)

# Conversion
clean_df = convert_to_numeric(clean_df)
clean_df = convert_to_datetime(clean_df)

# Validation & Rejection
rejected_df, valid_df = reject_products(clean_df)
```

**LOAD**
- **What**: Save processed data to destination
- **How**: Write DataFrames to CSV files in `data/processed/`
- **Output**: Multiple CSV files for different audiences
```python
valid_df.to_csv("data/processed/valid_products.csv")
rejected_df.to_csv("data/processed/rejected_products.csv")
summary_df.to_csv("data/processed/analytics_summary.csv")
```

---

## 📁 Project Structure
```
product-data-pipeline/
├── README.md
├── .gitignore
│
├── data/
│   ├── raw/ # Original source data
│   │   └── products_raw.csv
│   │
│   └── processed/  # Pipeline outputs
│       ├── clean_products.csv
│       ├── valid_products.csv
│       ├── rejected_products.csv
│       ├── review_products.csv
│       ├── analytics_summary.csv
│       └── price_analysis.csv
│
├── pipeline/
│   ├── __init__.py
│   ├── ingest.py  # Data ingestion
│   ├── transform.py  # Cleaning
│   └── analytics.py # Analysis
│
└── main.py # Pipeline orchestrator
```

---

## How to Run
```bash
# Install dependencies
uv sync

# Run the pipeline
uv run main.py
```

**Output**: All processed files will be saved to `data/processed/`

---

## Analytics & Insights

The pipeline generates the following analytical outputs:

### 1. **Analytics Summary** (`analytics_summary.csv`)
Compares clean vs valid datasets to measure the impact of data quality rules:

| Metric | Description |
|--------|-------------|
| `total_products_clean` | Total products after cleaning |
| `total_products_valid` | Products ready for production (passed all validations) |
| `missing_price` | Products with ID but no price |
| `median_price_clean` | Median price before rejection |
| `median_price_valid` | Median price after rejection (impact of filtering) |
| `mean_price_clean` | Average price before rejection |
| `mean_price_valid` | Average price after rejection |

### 2. **Price Analysis** (`price_analysis.csv`)
Identifies pricing anomalies and top-priced products:

- **Top 10 Expensive Products**: Highest-priced items in dataset
- **Top 10 Price Discrepancies**: Extreme outliers (IQR method) + products with missing prices (but with walid ID, currency and name)

### 3. **Valid Products** (`valid_products.csv`)
Clean, validated products ready for production use:
- Has valid ID, name, price, currency
- Price between 0-50,000 kr
- No duplicates
- Valid dates

### 4. **Rejected Products** (`rejected_products.csv`)
Products that failed validation with rejection reasons:
- Missing critical fields (ID, price, currency)
- Invalid price (≤ 0 or ≥ 50,000 kr)
- Duplicate IDs
- Invalid dates

### 5. **Products for Review** (`review_products.csv`)
Products flagged for manual review (not auto-rejected):
- Price outliers (statistical anomalies detected via IQR)
- Missing non-critical data
- Date inconsistencies
- Negative values

---

## Data Quality Checks

The pipeline performs the following validations:

| Check | Rule | Action |
|-------|------|--------|
| **Missing ID** | `id.isna()` | Reject |
| **Missing Price** | `price.isna()` | Reject |
| **Missing Currency** | `currency.isna()` | Reject |
| **Invalid Price** | `price ≤ 0 or price ≥ 50,000` | Reject |
| **Duplicates** | Duplicate `id` values | Flag for review |
| **Price Outliers** | IQR method (Q1 - 1.5×IQR, Q3 + 1.5×IQR) | Flag for review |
| **Date Violations** | `updated_at < created_at` | Flag for review |


---

## Pipeline Stages in Detail

### Stage 1: Ingestion
- Read CSV
- Preserve raw data integrity
- Log number of records ingested

### Stage 2: Cleaning
- Remove leading/trailing whitespace from all text columns
- Normalize text case (title case for names, uppercase for currency codes)
- Standardize date formats (convert `/` to `-`)

### Stage 3: Type Conversion
- Convert price to numeric (invalid values → NaN)
- Convert dates to datetime objects
- Convert currency to categorical type

### Stage 4: Flagging
- Identify missing data across all columns
- Detect date violations (logical inconsistencies)
- Find duplicate IDs
- Flag negative values
- Identify price outliers using statistical methods (IQR)

### Stage 5: Rejection
- Apply business rules to separate valid from invalid products
- Add rejection reasons for transparency
- Generate review list for edge cases

### Stage 6: Analytics
- Calculate summary statistics (mean, median, count)
- Compare clean vs valid datasets
- Identify top expensive products
- Flag pricing anomalies

---

## Learning Outcomes

This project demonstrates:
- ETL pipeline design and implementation
- Data quality assessment and validation
- Statistical outlier detection
- DataFrame operations and transformations
- Modular Python code organization
- CSV data processing at scale
- Analytical report generation

---
