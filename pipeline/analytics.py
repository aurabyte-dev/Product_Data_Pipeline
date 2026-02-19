

import pandas as pd
from transform import price_outliers_high

# Generate summary statistics
def generate_analytics_summary(clean_df, valid_df):


# PRODUCT COUNT
# Calculate product count # calculated on both valid and clean to see which product can go live now
product_count_clean = clean_df["id"].notna().sum()
product_count_valid = valid_df["id"].notna().sum()


# Count products with ID but missing price
# only valid for cleaned_df since products without price are rejected from valid_df
mssing_price_count = (clean_df["id"].notna() & clean_df["price"].isna()).sum()


# MEAN & MEDIAN
# calculated on both both valid och clen to anamlyze the effect of the flagged products 
count_median_valid = valid_df["price"].median()
count_mean_valid = valid_df["price"].mean()
count_median_clean = clean_df["price"].median()
count_mean_clean = clean_df["price"].mean()


# PRICE ANALYCIS

# Top 10 most expensiva products
top_10_expensive = clean_df.nlargest(10, "price")

# Price outliers with IQR method (7 products)
extreme_outliers = clean_df[(clean_df["price"] < Q1) | (clean_df["price"] > upper_bound)]
print(extreme_outliers[["name", "price"]].sort_values("price", ascending=False))

# Products with ID and name but no price
price_missing = clean_df[
    clean_df["price"].isna() & 
    clean_df["name"].notna() &
    clean_df["id"].notna()]
print(price_missing[["name", "id", "price"]])

# Top 10 price discrepancies
top_10_price_discrepancies = (
    pd.concat([extreme_outliers, price_missing])
    .drop_duplicates()
    .sort_values("price", ascending=False, na_position='last')
    .head(10))
print(top_10_price_discrepancies[["id", "name", "price"]])
#________________________________

# Create a data frames for:

# ANALYTICS SUMMARY

analytics_summary = pd.DataFrame({
    'total_products_clean': [product_count_clean],
    'total_products_valid': [product_count_valid],
    'missing_price': [mssing_price_count],
    'median_price_clean': [round(count_median_clean, 2)],
    'median_price_valid': [round(count_median_valid, 2)],
    'mean_price_clean': [round(count_mean_clean, 2)],
    'mean_price_valid': [round(count_mean_valid, 2)]
})
return summary_df


# PRICE ANALYSIS
price_analysis = pd.DataFrame({
    'top_10_highest_price': [top_10_expensive],
    'top_10_price_discrepancies': [top_10_price_discrepancies]
})
return analysis_df