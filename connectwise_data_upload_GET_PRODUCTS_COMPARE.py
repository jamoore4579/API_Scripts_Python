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

# Function to get products based on identifier from the API
def get_product_by_identifier(identifier):
    try:
        # Parameters for filtering the API response
        params = {
            "pageSize": 1000,
            "conditions": f'catalogItem/identifier="{identifier}"',  # Dynamically set identifier
            "fields": "id,catalogItem/identifier,description,opportunity/id,salesOrder/id,agreement/id"  # Include specified fields
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
            print(f"No product data found for identifier: {identifier}")
            return None

        # Normalize data and flatten nested fields
        products_df = pd.json_normalize(products_data)

        # Handle missing columns and ensure proper renaming
        products_df.rename(columns={
            'catalogItem.identifier': 'catalogItem',
            'opportunity.id': 'opportunity',
            'salesOrder.id': 'salesOrder',
            'agreement.id': 'agreement'
        }, inplace=True)

        # Convert 'opportunity', 'salesOrder', and 'agreement' columns to string dtype before filling
        if 'opportunity' not in products_df.columns:
            products_df['opportunity'] = 'None'
        else:
            products_df['opportunity'] = products_df['opportunity'].astype(str).fillna('None')

        if 'salesOrder' not in products_df.columns:
            products_df['salesOrder'] = 'None'
        else:
            products_df['salesOrder'] = products_df['salesOrder'].astype(str).fillna('None')

        if 'agreement' not in products_df.columns:
            products_df['agreement'] = 'None'
        else:
            products_df['agreement'] = products_df['agreement'].astype(str).fillna('None')

        # Order columns as specified
        column_order = ["catalogItem", "description", "opportunity", "salesOrder", "agreement"]
        products_df = products_df.reindex(columns=column_order, fill_value='')

        return products_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching products from API for identifier {identifier}: {e}")
        return None

# Function to read identifiers from CSV file
def read_identifiers_from_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        if 'identifier' not in df.columns:
            print("CSV does not contain 'identifier' column.")
            return None
        return df['identifier'].tolist()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

# Function to append product data to CSV file
def write_products_to_csv(dataframe, file_path):
    try:
        if os.path.isfile(file_path):
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Product data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

# Main function to loop over identifiers and fetch product data
def upload_product_data():
    input_file_path = r'c:\users\jmoore\documents\connectwise\projects\product_data.csv'  # Path to CSV with identifiers
    output_file_path = r'c:\users\jmoore\documents\connectwise\projects\update_product_data.csv'  # Output CSV path

    identifiers = read_identifiers_from_csv(input_file_path)
    if not identifiers:
        print("No identifiers to process.")
        return

    for identifier in identifiers:
        products_df = get_product_by_identifier(identifier)
        if products_df is not None:
            write_products_to_csv(products_df, output_file_path)

# Call the function to load and upload product data
upload_product_data()
