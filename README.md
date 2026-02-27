# Product Data Pipeline

ETL pipeline that ingests product CSV data, validates quality, flags problematic records, and generates analytical reports.
Built to get hands-on with real data quality problems — messy inputs, rejection logic, and the kind of edge cases that break simple pipelines.

---

## Stack 
* Python, Pandas, Pydantic

---

## How to Run
```bash
uv sync
uv run main.py
```
Processed files are saved to `data/processed/`

---

## What it does

The pipeline runs product data through cleaning, validation, and analytics. Valid products pass through, bad ones get rejected with a reason, and edge cases get flagged for manual review.

### Data Quality Checks

| Check | Rule | Action |
|-------|------|--------|
| **Missing ID** | `id.isna()` | Reject |
| **Missing Price** | `price.isna()` | Reject |
| **Missing Currency** | `currency.isna()` | Reject |
| **Invalid Price** | `price ≤ 0 or price ≥ 50,000` | Reject |
| **Duplicates** | Duplicate `id` values | Flag for review |
| **Price Outliers** | IQR method | Flag for review |
| **Date Violations** | `updated_at < created_at` | Flag for review |

---

### Outputs

| File | What's in it 
|------|--------------|
|`valid_products.csv` | Clean products that passed all checks |
| `rejected_products.csv` | Failed products with rejection reasons |
| `review_products.csv` | Edge cases flagged for manual review |
| `analytics_summary.csv` | Clean vs valid comparison metrics |
| `id` values | Flag for review |
| `price_analysis.csvt` |Outliers and top 10 most expensive products |

---

_Built as part of my data engineering studies. Focused on practicing ETL patterns and data quality logic in Python._

---
