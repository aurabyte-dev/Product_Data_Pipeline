### Data cleaning functions ###

import pandas as pd

def clean_whitespace(raw_df):
    # Remove whitespace from string columns

    clean_df = raw_df.copy()

    string_colums = clean_df.select_dtypes(include=['object']).columns
    for col in string_colums:
        clean_df[col] = clean_df[col].str.strip()
    
    return clean_df


def case_normalization(clean_df):
    # Change title to uppercase

    clean_df = clean_df.copy()

    string_colums = clean_df.select_dtypes(include=['object']).columns
    for col in string_colums:
        clean_df[col] = clean_df[col].str.title()
    
    return clean_df
