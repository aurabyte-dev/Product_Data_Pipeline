import pandas as pd


def generate_analytics_summary(clean_df, valid_df):
    """Compare clean vs valid datasets to measure impact of rejection rules."""

    # Product counts: clean = all processed, valid = ready to go live (has ID, price, currency and price is between 0-50k)
    product_count_clean = clean_df["id"].notna().sum()
    product_count_valid = valid_df["id"].notna().sum()

    # Missing price (clean only - these are already rejected from valid)
    mssing_price_count = (clean_df["id"].notna() & clean_df["price"].isna()).sum()

    # Price stats on both sets to see effect of filtering
    count_median_valid = valid_df["price"].median()
    count_mean_valid = valid_df["price"].mean()
    count_median_clean = clean_df["price"].median()
    count_mean_clean = clean_df["price"].mean()


    # Create summary DataFrame
    summary_df = pd.DataFrame({
        'total_products_clean': [product_count_clean],
        'total_products_valid': [product_count_valid],
        'missing_price': [mssing_price_count],
        'median_price_clean': [round(count_median_clean, 2)],
        'median_price_valid': [round(count_median_valid, 2)],
        'mean_price_clean': [round(count_mean_clean, 2)],
        'mean_price_valid': [round(count_mean_valid, 2)]
    })
    return summary_df


def generate_price_analysic(clean_df):
    """Identify top priced products and price discrepancies."""

    # Top 10 most expensive products
    top_10_expensive = clean_df.nlargest(10, "price")[["id", "price"]].reset_index(drop=True)
    
    # IQR-based outlier detection
    Q1 = clean_df["price"].quantile(0.25)
    Q3 = clean_df["price"].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 1.5 * IQR

    extreme_outliers = clean_df[
        (clean_df["price"] < Q1) |
        (clean_df["price"] > upper_bound)]

    # Products with ID and name but no price
    price_missing = clean_df[
        clean_df["price"].isna() &
        clean_df["name"].notna() &
        clean_df["id"].notna()]

    # Combine outliers and missing prices, keep worst 10
    top_10_price_discrepancies = (
        pd.concat([extreme_outliers, price_missing])
        .drop_duplicates()
        .sort_values("price", ascending=False, na_position='last')
        .head(10)[["id", "price"]]
        .reset_index(drop=True)
    )
    
    # Create analysis DataFrame
    analysis_df = pd.DataFrame({
        'expensive_id': top_10_expensive["id"],
        'exoensive_price': top_10_expensive["price"],
        'discrepancy_id': top_10_price_discrepancies["id"],
        'discrepancy_price': top_10_price_discrepancies["price"]
    })
    return analysis_df
