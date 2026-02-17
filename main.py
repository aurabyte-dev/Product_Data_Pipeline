import pandas as pd


if __name__ == "__main__":

    products_df = pd.read_csv("data/raw/products_raw.csv")


    print(products_df.head())
    print(f"Ingested {len(products_df)} products")