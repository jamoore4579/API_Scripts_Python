import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

# File paths
input_file_path = r'c:\users\jmoore\documents\connectwise\integration\Keep_Products.csv'
output_file_path = r'c:\users\jmoore\documents\connectwise\integration\Keep_Products_updated.csv'

def get_catalog_items_from_csv(file_path):
    try:
        # Read the CSV file to extract 'catalogItem' column values
        data = pd.read_csv(file_path)
        if 'catalogItem' in data.columns:
            # Return the unique 'catalogItem' values as a list
            return data['catalogItem'].dropna().unique().tolist()
        else:
            print(f"Column 'catalogItem' not found in {file_path}.")
            return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def get_products(catalog_items):
    try:
        # Make the API request for each catalog item
        product_list = []

        for item in catalog_items:
            # Parameters for the API request
            params = {
                "fields": "id,identifier,description",
                "conditions": f"identifier like \"{item}%\""  # Apply condition to filter identifiers based on the catalog item
            }
            
            response = requests.get(url=f"{BASE_URL}/procurement/catalog", headers=headers, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors

            response_data = response.json()

            # Handle response format, which may be a list or dictionary
            if isinstance(response_data, list):
                product_list.extend(response_data)
            elif isinstance(response_data, dict):
                # If response is a dictionary, extract the "items" key if it exists
                product_list.extend(response_data.get("items", []))
            else:
                print("Unexpected response format.")

        # Convert the list of product data into a DataFrame
        if product_list:
            return pd.DataFrame(product_list)
        else:
            print("No product data found for the given catalog items.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching products from API: {e}")
        return None

def write_products_to_csv(dataframe, file_path):
    try:
        # Check if the file already exists
        if os.path.isfile(file_path):
            # Append to the CSV file without writing the header
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            # Write to a new CSV file with the header
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Product data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_product_data():
    # Get catalog items from the input CSV file
    catalog_items = get_catalog_items_from_csv(input_file_path)

    # If there are catalog items, fetch product data using the API
    if catalog_items:
        products_df = get_products(catalog_items)
        
        # If data exists, write to CSV
        if products_df is not None:
            write_products_to_csv(products_df, output_file_path)
        else:
            print("No data to write to CSV.")
    else:
        print("No catalog items found in the CSV file.")

# Call the function to load and upload product data
upload_product_data()
