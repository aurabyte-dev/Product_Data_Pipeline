import pandas as pd
from pipeline.ingest import ingest_products
from pipeline import transform as tf
from pipeline import analytics as a

if __name__ == "__main__":

    # STEP 1: INGEST
    raw_df = ingest_products()
    print(f"Ingested {len(raw_df)} products")

    # STEP 2: CLEAN
    clean_df = tf.clean_whitespace(raw_df)
    clean_df = tf.case_normalization(clean_df)
    clean_df = tf.standardize_date_format(clean_df)
    print("Cleaning complete")
   
    # STEP 3: CONVERT
    clean_df = tf.currency_to_categorical(clean_df)
    clean_df = tf.convert_to_datetime(clean_df)
    clean_df = tf.convert_to_numeric(clean_df)
    print("Conversion complete")

    # STEP 4: FLAG
    clean_df = tf.missing_data(clean_df)
    clean_df = tf.date_violation(clean_df)
    clean_df = tf.duplicates(clean_df)
    clean_df = tf.negative_value(clean_df)
    clean_df = tf.price_outliers_high(clean_df)
    review_df = tf.categorize_for_review(clean_df)

    clean_df.to_csv("data/processed/cleaned_products.csv", index=False)
    review_df.to_csv("data/processed/review_products.csv", index=False)

    print("Flaging complete")
    print(f"Saved {len(review_df)} products for review\n")

    # STEP 5: REJECT
    rejected_df, valid_df = tf.reject_products(clean_df)
    rejected_df.to_csv("data/processed/rejected_products.csv", index=False)
    print(f"Saved {len(rejected_df)} rejected products\n")


    # STEP 6: LOAD
    valid_df.to_csv("data/processed/valid_products.csv", index=False)
    print(f"Saved {len(valid_df)} valid products\n")
    clean_df.to_csv("data/processedclean_df.csv", index=False)
    summary_df = a.generate_analytics_summary(clean_df, valid_df)
    summary_df.to_csv("data/processed/analytics_summary.csv", index=False)
    print(f"Saved summary report")
    analysis_df = a.generate_price_analysic(clean_df)
    analysis_df.to_csv("data/processed/price_analysis.csv", index=False)
    print(f"Saved analytics report")
    

