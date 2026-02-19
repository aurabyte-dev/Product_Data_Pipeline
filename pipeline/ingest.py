""" INGESTION FUNCTION """

import pandas as pd
from pathlib import Path

# Load raw products CSV from data/raw/

def ingest_products():
    
    # 1. Find the path
    csv_path = Path("data/raw/products_raw.csv")

    # 2. Verify that the file exists
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found:{csv_path}")
    
    # 3. Ingest data
    raw_df = pd.read_csv(csv_path)
    print(f"Ingested {len(raw_df)} products from {csv_path}")
    print(raw_df.head())

    return raw_df

if __name__ == "__main__":
    ingest_products()