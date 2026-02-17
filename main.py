import pandas as pd
from pipeline.ingest import ingest_products
from pipeline.clean import case_normalization


if __name__ == "__main__":

    # STEP 1: INGEST
    raw_df = ingest_products()
    print(f"Ingested {len(raw_df)} products")

    # STEP 2: CLEAN
    clean_df = case_normalization



    