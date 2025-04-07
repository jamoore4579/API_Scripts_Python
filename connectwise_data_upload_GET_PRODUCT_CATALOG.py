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

# Output file paths
catalog_output_path = r'c:\users\jmoore\documents\connectwise\products\lenovoproducts.csv'
product_details_output_path = r'c:\users\jmoore\documents\connectwise\products\lenovoproduct_details.csv'

# Function to retrieve detailed product info by catalog item ID
def get_product_details_by_catalog_id(catalog_ids):
    product_details = []
    for catalog_id in catalog_ids:
        params = {
            "conditions": f"catalogItem/id = {catalog_id}",
            "fields": "catalogItem/id,catalogItem/identifier,opportunity/id,salesOrder/id"
        }
        response = requests.get(f"{BASE_URL}/procurement/products", headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data:  # Only add non-empty results
                product_details.extend(data)
        else:
            print(f"Failed to get product for catalog ID {catalog_id}: {response.status_code}")
    return product_details

# --- Main script logic ---
# Get catalog items filtered by Lenovo manufacturer
catalog_params = {
    "conditions": 'manufacturer/name like "lenovo%"',
    "fields": "id,identifier,manufacturer/name"
}
response = requests.get(f"{BASE_URL}/procurement/catalog", headers=headers, params=catalog_params)

if response.status_code == 200:
    catalog_items = response.json()
    
    # Normalize catalog item response
    df_catalog = pd.json_normalize(catalog_items)
    
    # Rename manufacturer.name column
    df_catalog.rename(columns={"manufacturer.name": "Manufacturer Name"}, inplace=True)
    
    # Save catalog items to CSV
    df_catalog.to_csv(catalog_output_path, index=False)
    print(f"Catalog data saved to {catalog_output_path}")
    
    # Get product details for each catalog item
    catalog_ids = df_catalog['id'].tolist()
    product_details = get_product_details_by_catalog_id(catalog_ids)
    
    # Convert product details to DataFrame and export
    if product_details:
        df_products = pd.json_normalize(product_details)
        df_products.to_csv(product_details_output_path, index=False)
        print(f"Product details saved to {product_details_output_path}")
    else:
        print("No product details found.")
else:
    print(f"Failed to retrieve catalog items: {response.status_code} - {response.text}")
