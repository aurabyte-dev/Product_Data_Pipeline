"""Product data transformation functions.

Transforms raw product data through four sequential stages:

1. **Clean** — Normalize string formatting (whitespace, casing, date separators)
2. **Convert** — Cast columns to correct dtypes (datetime, numeric, categorical)
3. **Flag** — Add boolean columns identifying data quality issues
4. **Reject** — Split products into valid and rejected DataFrames

Expected columns: id, name, price, currency, created_at, updated_at

Each function takes a DataFrame, returns a copy (never mutates the input).
"""

import pandas as pd
import numpy as np


#=========================================#
#                  CLEAN                  #
#=========================================#

# REMOVE WHITESPACE - [string colums]

def clean_whitespace(raw_df):
    clean_df = raw_df.copy()
    string_colums = clean_df.select_dtypes(include=['object']).columns
    for col in string_colums:
        clean_df[col] = clean_df[col].str.strip()
    return clean_df




# NORMALIZE TO TITLE CASE - [name]

def case_normalization(clean_df):
    clean_df = clean_df.copy()
    clean_df["name"] = clean_df["name"].str.title()
    return clean_df



 # STANDARDIZE FORMAT - [date_colums]

def standardize_date_format(clean_df):
    clean_df = clean_df.copy()
    date_columns = ["created_at", "updated_at"]
    for col in date_columns:
        clean_df[col] = clean_df[col].str.replace('/','-')
    return clean_df



#=========================================#
#                CONVERT                  #
#=========================================#


# CONVERT TO CATEGORICAL - [currency] 

def currency_to_categorical(clean_df):
    clean_df = clean_df.copy()
    clean_df["currency"] = clean_df["currency"].astype("category")
    return clean_df



# CONVERT TO DATETIME - [date colums]

def convert_to_datetime(clean_df):
    clean_df = clean_df.copy()
    date_columns = ["created_at", "updated_at"]
    for col in date_columns:
        clean_df[col] = pd.to_datetime(clean_df[col], errors="coerce")
    return clean_df


# CONVERT TO NUMERIC - [price]

def convert_to_numeric(clean_df):
    clean_df = clean_df.copy()
    clean_df["price"] = pd.to_numeric(clean_df["price"], errors="coerce")
    return clean_df



#=========================================#
#                   FLAG                  #
#=========================================#


# MISSING DATA - [all columns]

def missing_data(clean_df):
    clean_df = clean_df.copy()
    for col in clean_df.columns:
        clean_df["missing_data"] = clean_df.isna().any(axis=1)
    return clean_df



# DATE VIOLATION - [date colums]

def date_violation(clean_df):
    clean_df = clean_df.copy()
    if "updated_at" in clean_df.columns:
        clean_df["date_violation"] = clean_df["updated_at"] < clean_df["created_at"]
    return clean_df



# DUPLICATES - [all colums]

def duplicates(clean_df):
    clean_df = clean_df.copy()
    clean_df["duplicates"] = (
        clean_df.duplicated(subset = ["id"], keep=False) &
        clean_df["id"].notna())
    return clean_df



# ZERO OR NEGATIVE VALUE - [price]

def negative_value(clean_df):
    clean_df = clean_df.copy()
    clean_df["negative_price"] = clean_df["price"] <= 0
    return clean_df



# HIGH VALUE using IQR method - [price]

def price_outliers_high(clean_df): 
    clean_df = clean_df.copy()

    # 1. Calculate IQR
    Q1 = clean_df["price"].quantile(0.25)  # 25% percentile
    Q3 = clean_df["price"].quantile(0.75)  # 75% percentile
    IQR = Q3 - Q1                          # Difference between them

    # 2. Set upper bound
    upper_bound = Q3 + 1.5 * IQR

    # 3. Flag outliers
    clean_df["price_outlier"] = clean_df["price"] > upper_bound
    return clean_df



# CREATE NEW DATAFRAME - FLAGED PRODUCTS 

def  categorize_for_review(clean_df):
    clean_df = clean_df.copy()
    
    clean_df["review_reason"] = ""
    
    clean_df.loc[clean_df["missing_data"], "review_reason"] += "Missing data; "
    clean_df.loc[clean_df["negative_price"], "review_reason"] += "Negative price; "
    clean_df.loc[clean_df["price_outlier"], "review_reason"] += "Price outlier; "
    clean_df.loc[clean_df["duplicates"], "review_reason"] += "Duplicate ID; "
    clean_df.loc[clean_df["date_violation"], "review_reason"] += "Invalid date; "
    
    needs_review = clean_df["review_reason"] != ""

    # Create new dataframe
    review_df = clean_df[needs_review].copy()

    return review_df



#=========================================#
#                 REJECT                  #
#=========================================#

def reject_products(clean_df):
    clean_df = clean_df.copy()

    # Define rejection criteria
    reject_condition = (
        (clean_df["id"].isna()) |
        (clean_df["currency"].isna()) |
        (clean_df["price"] <= 0) |
        (clean_df["price"] > 50000 )
    )
    
    # Separate
    rejected_df = clean_df[reject_condition].copy()
    valid_df = clean_df[~reject_condition].copy()

    # Add rejection reasons
    rejected_df["rejection_reason"] = ""
    rejected_df.loc[rejected_df["id"].isna(), "rejection_reason"] += "Missing ID; "
    rejected_df.loc[rejected_df["currency"].isna(), "rejection_reason"] += "Missing currency; "
    rejected_df.loc[rejected_df["price"] <= 0, "rejection_reason"] += "Invalid price (≤0); "
    rejected_df.loc[rejected_df["price"] > 50000, "rejection_reason"] += "Extreme price (>50k); "
    
    return rejected_df, valid_df