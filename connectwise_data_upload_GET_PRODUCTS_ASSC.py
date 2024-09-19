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

def get_products():
    try:
        # Parameters for filtering the API response
        params = {
            "pageSize": 1000,
            "conditions": 'catalogItem/identifier="0202-004"',  # Correct format for conditions
            "fields": "id,catalogItem/identifier,description,opportunity/id,salesOrder/id"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/procurement/products", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse response data
        response_data = response.json()
        if isinstance(response_data, list):
            products_data = response_data
        elif isinstance(response_data, dict):
            products_data = response_data.get("items", [])
        else:
            print("Unexpected response format.")
            return None

        if not products_data:
            print("No product data found.")
            return None

        # Normalize data and flatten nested fields
        products_df = pd.json_normalize(products_data)

        # Handle missing columns and ensure proper renaming
        products_df.rename(columns={
            'catalogItem.identifier': 'catalogItem',
            'opportunity.id': 'opportunity',
            'salesOrder.id': 'salesOrder'
        }, inplace=True)

        # Order columns as specified
        column_order = ["id", "catalogItem", "description", "opportunity", "salesOrder"]
        products_df = products_df.reindex(columns=column_order, fill_value='')  # Fill missing columns with empty string
        
        return products_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching products from API: {e}")
        return None

def write_products_to_csv(dataframe, file_path):
    try:
        if os.path.isfile(file_path):
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Product data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_product_data():
    products_df = get_products()
    if products_df is not None:
        results_file_path = r'c:\users\jmoore\documents\connectwise\projects\product_data.csv'
        write_products_to_csv(products_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload product data
upload_product_data()
