""" Data cleaning and convertion functions """

import pandas as pd
import numpy as np

"""------ CLEANING FUNCTIONS --------"""

def clean_whitespace(raw_df):

    # Remove whitespace from string columns
    clean_df = raw_df.copy()

    string_colums = clean_df.select_dtypes(include=['object']).columns
    for col in string_colums:
        clean_df[col] = clean_df[col].str.strip()
    
    return clean_df


def case_normalization(clean_df):
    
    # Change name title to uppercase
    clean_df = clean_df.copy()

    clean_df["name"] = clean_df["name"].str.title()
    
    return clean_df


def standardize_date_format(clean_df):

    # Standardize date format
    clean_df = clean_df.copy()

    date_columns = ["created_at", "updated_at"]
    for col in date_columns:
        clean_df[col] = clean_df[col].str.replace('/','-')

    return clean_df


"""------ CONVERTION FUNCTIONS --------"""

# Convert currency to categorical 

def currency_to_categorical(clean_df):
    clean_df = clean_df.copy()
    clean_df["currency"] = clean_df["currency"].astype("category")
    return clean_df


# Convert date to datetime

def convert_to_datetime(clean_df):
    clean_df = clean_df.copy()
    date_columns = ["created_at", "updated_at"]
    for col in date_columns:
        clean_df[col] = pd.to_datetime(clean_df[col], errors="coerce")
    return clean_df


# Convert price to numeric

def convert_to_numeric(clean_df):
    clean_df = clean_df.copy()
    clean_df["price"] = pd.to_numeric(clean_df["price"], errors="coerce")
    return clean_df