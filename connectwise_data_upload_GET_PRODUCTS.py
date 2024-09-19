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
            "conditions": "type/id=34 AND (inactiveFlag = false) AND (id>=11089 AND id<=12010)",  # Filter by type id and product ID range
            "fields": "id,identifier,inactiveFlag,type,category/name,subcategory/name"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/procurement/catalog", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Check if the response is a list or dictionary
        response_data = response.json()
        if isinstance(response_data, list):
            # If the response is a list, treat it as the product data
            products_data = response_data
        elif isinstance(response_data, dict):
            # If it's a dictionary, get the "items" key if available
            products_data = response_data.get("items", [])
        else:
            # Handle unexpected response format
            print("Unexpected response format.")
            return None

        # Check if product data is available
        if not products_data:
            print("No product data found.")
            return None

        # Load product data into a DataFrame
        results_df = pd.DataFrame(products_data)
        
        # Order columns as specified
        column_order = ["id", "identifier", "inactiveFlag", "type", "category", "subcategory"]
        results_df = results_df.reindex(columns=column_order)
        
        return results_df

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
    # Get product data from API
    products_df = get_products()

    # If data exists, write to CSV
    if products_df is not None:
        results_file_path = r'c:\users\jmoore\documents\connectwise\projects\product_data.csv'
        write_products_to_csv(products_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload product data
upload_product_data()
