import pandas as pd
import numpy as np
import os
import datetime

# Function to handle CSV loading with error handling, using Python engine for parsing
def load_csv(file_path, column_names, dtype=None, encoding=None):
    try:
        df = pd.read_csv(file_path, usecols=column_names, dtype=dtype, encoding=encoding, on_bad_lines='skip', engine='python')
        return df
    except pd.errors.ParserError as e:
        print(f"Error reading the CSV file '{file_path}': {e}")
        return None

def clean_price_column(df, column_name):
    # Replace commas and convert the column to numeric, forcing invalid parsing to NaN
    df[column_name] = df[column_name].replace({',': ''}, regex=True)
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    return df

def main():
    # Specify the data types for the columns
    dtype = {
        'sku': str,
        'price': str,  # Load price as string initially for cleaning
    }

    # Load the Magento dataframe
    magento_df = load_csv('cuda_export_catalog_product_20240914_094209.csv',
                          ['sku', 'product_online', 'price'], dtype=dtype)

    if magento_df is None:
        print("Failed to load Magento dataframe.")
        return  # Stop execution if magento_df could not be loaded

    # Load the Sophos dataframe
    sophos_df = load_csv('cuda-sep-24.csv',
                         ['sku', 'Description', 'price'], dtype=dtype)

    if sophos_df is None:
        print("Failed to load Sophos dataframe.")
        return  # Stop execution if sophos_df could not be loaded

    # Clean and convert the price columns to numeric values
    magento_df = clean_price_column(magento_df, 'price')
    sophos_df = clean_price_column(sophos_df, 'price')

    # Rename price columns for clarity in merged DataFrame
    magento_df.rename(columns={'price': 'price_magento'}, inplace=True)
    sophos_df.rename(columns={'price': 'price_sophos'}, inplace=True)

    # Merge the two dataframes based on 'sku', using left join to ensure all Magento products are included
    merged_df = pd.merge(magento_df, sophos_df, on='sku', how='left', suffixes=('_magento', '_sophos'))

    # Debugging: Print the merged DataFrame
    print("Merged DataFrame:")
    print(merged_df.head())

    # Filtering the price differences where 'product_online' is 1 and prices differ
    price_diff_df = merged_df[
        (merged_df['product_online'] == 1) &
        (merged_df['price_magento'] != merged_df['price_sophos']) &
        merged_df['price_sophos'].notna()
    ][['sku', 'price_sophos']]

    # Renaming and adding columns for output
    price_diff_df.columns = ['sku', 'price']
    price_diff_df['store_view_code'] = np.nan
    price_diff_df['product_websites'] = "base"

    # Create dynamic folder for saving files
    folder_name = 'Cuda'
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dynamic_folder_name = f"{folder_name}-{today_date}"
    if not os.path.exists(dynamic_folder_name):
        os.makedirs(dynamic_folder_name)

    # Dynamic filenames with date suffix
    price_diff_filename = f"{dynamic_folder_name}/price-difference-{today_date}.csv"
    disable_sku_filename = f"{dynamic_folder_name}/disable-sku-{today_date}.csv"
    missing_sku_filename = f"{dynamic_folder_name}/missing-sku-{today_date}.csv"

    # Create price-difference.csv
    price_diff_df.to_csv(price_diff_filename, index=False)

    # Handle SKUs with no price in Sophos (disable-sku.csv)
    disable_sku_df = merged_df[
        (merged_df['product_online'] == 1) &
        merged_df['price_sophos'].isna()
    ][['sku']]
    disable_sku_df['product_online'] = 2
    disable_sku_df['store_view_code'] = np.nan
    disable_sku_df['product_websites'] = "base"
    disable_sku_df.to_csv(disable_sku_filename, index=False)

    # Handle missing SKUs from Magento but found in Sophos (missing-sku.csv)
    missing_sku_df = merged_df[merged_df['product_online'].isna()][['sku', 'Description', 'price_sophos']]
    missing_sku_df.columns = ['sku', 'Description', 'price']
    missing_sku_df.to_csv(missing_sku_filename, index=False)

if __name__ == "__main__":
    main()
