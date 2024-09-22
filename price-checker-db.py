import sqlite3
import pandas as pd
import os
import datetime

# Function to load data from SQLite
def load_sqlite_table(db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        # Properly quoting table names that contain special characters
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except sqlite3.Error as e:
        print(f"Error reading from SQLite database: {e}")
        return None

def main():
    # Database path
    db_path = r'C:\Users\crash\OneDrive\Desktop\firewalls\db\firewalls_database.db'

    # Load Magento table (meg-sonicwall-sep-24)
    magento_df = load_sqlite_table(db_path, 'meg-sonicwall-sep-24')

    if magento_df is None:
        print("Failed to load Magento dataframe.")
        return  # Stop execution if magento_df could not be loaded

    # Load SonicWall table (sonicwall-sep-24)
    sophos_df = load_sqlite_table(db_path, 'sonicwall-sep-24')

    if sophos_df is None:
        print("Failed to load Sophos dataframe.")
        return  # Stop execution if sophos_df could not be loaded

    # Debugging: Print the loaded dataframes
    print("Magento DataFrame:")
    print(magento_df.head())

    print("SonicWall DataFrame:")
    print(sophos_df.head())

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
    price_diff_df['store_view_code'] = None
    price_diff_df['product_websites'] = "base"

    # Create dynamic folder for saving files
    folder_name = 'SW'
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
    disable_sku_df['store_view_code'] = None
    disable_sku_df['product_websites'] = "base"
    disable_sku_df.to_csv(disable_sku_filename, index=False)

    # Handle missing SKUs from Magento but found in Sophos (missing-sku.csv)
    missing_sku_df = merged_df[merged_df['product_online'].isna()][['sku', 'Description', 'price_sophos']]
    missing_sku_df.columns = ['sku', 'Description', 'price']
    missing_sku_df.to_csv(missing_sku_filename, index=False)

if __name__ == "__main__":
    main()
