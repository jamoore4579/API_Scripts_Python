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

def get_product_by_identifier(product_name):
    """
    Get product data from the API based on a single product identifier.
    """
    try:
        # Define the URL for the specific API request
        request_url = f"{BASE_URL}/procurement/catalog"
        
        # Define the condition for filtering by a single product name
        params = {
            "conditions": f"(identifier like \"{product_name}%\")",  # Filter by identifier starting with the product_name
            "fields": "id,identifier,inactiveFlag,type,category/name,subcategory/name"  # Include specified fields
        }

        # Make the API request
        response = requests.get(url=request_url, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the response data
        response_data = response.json()
        if isinstance(response_data, list):
            products_data = response_data
        elif isinstance(response_data, dict):
            products_data = response_data.get("items", [])
        else:
            return None

        return products_data

    except requests.exceptions.RequestException:
        return None

def upload_product_data(file_path):
    """
    Upload product data from the specified CSV file.
    """
    try:
        # Read data from the provided CSV file path
        data = pd.read_csv(file_path)

        # Check if 'product_name' column exists in the CSV file
        if 'product_name' not in data.columns:
            return None

        # Extract the 'product_name' column data
        product_names = data['product_name'].tolist()
        return product_names
    except Exception:
        return None

def get_combined_product_data(product_names):
    """
    Retrieve product data for each product name individually and combine them.
    """
    all_products = []

    # Iterate through each product name and retrieve data
    for product_name in product_names:
        product_data = get_product_by_identifier(product_name)
        if product_data:
            all_products.extend(product_data)  # Append product data to the combined list

    # If any product data was found, load it into a DataFrame
    if all_products:
        results_df = pd.DataFrame(all_products)

        # Add the product_name column by copying the identifier values
        results_df['product_name'] = results_df['identifier']

        # Reorder columns to include product_name
        column_order = ["id", "identifier", "product_name", "inactiveFlag", "type", "category", "subcategory"]
        results_df = results_df.reindex(columns=column_order)

        return results_df
    else:
        return None

def write_products_to_csv(dataframe, file_path):
    """
    Write the combined product data to a CSV file.
    """
    try:
        # Write to a new CSV file with the header
        dataframe.to_csv(file_path, mode='w', header=True, index=False)
    except Exception as e:
        print(f"Error writing to file: {e}")

def main():
    # Read product names from an existing CSV file
    inactivate_file_path = r'c:\users\jmoore\documents\connectwise\integration\inactivate_product.csv'
    product_names = upload_product_data(inactivate_file_path)

    # Get product data from API for each identifier separately and save to CSV
    if product_names:
        combined_products_df = get_combined_product_data(product_names)
        if combined_products_df is not None:
            results_file_path = r'c:\users\jmoore\documents\connectwise\integration\product_data.csv'
            write_products_to_csv(combined_products_df, results_file_path)

# Call the main function to execute the script
main()
