import os
import pandas as pd
import requests
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Check if essential environment variables are set
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    print("Environment variables missing: Check BASE_URL, AUTH_CODE, and CLIENT_ID")
    sys.exit(1)

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load the CSV file paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\All_Items_103124.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\All_Items_103124_Update.csv"

# Define columns to load
columns_to_load = ["ProductID", "category", "subCategory", "ProductType", "Class", "Serialized", "Manufacturer", "RenewableItem", "SpecialOrderItem"]

# Read CSV file and retrieve specific columns
try:
    df = pd.read_csv(input_file_path, usecols=columns_to_load)
except FileNotFoundError:
    print(f"CSV file not found at path: {input_file_path}")
    sys.exit(1)
except ValueError as e:
    print(f"Error reading CSV file: {e}")
    sys.exit(1)

# Check if DataFrame is empty
if df.empty:
    print("DataFrame is empty after reading CSV. Exiting.")
    sys.exit(1)

# Replace NaN values with empty strings to ensure JSON compatibility
df = df.fillna("")

# Conversion function for RenewableItem and SpecialOrderItem values
def yes_no_to_bool(value):
    if isinstance(value, str):
        return True if value.lower() == 'yes' else False
    return value

# Apply conversion to RenewableItem and SpecialOrderItem columns
df['RenewableItem'] = df['RenewableItem'].apply(yes_no_to_bool)
df['SpecialOrderItem'] = df['SpecialOrderItem'].apply(yes_no_to_bool)

# Function to make a GET request
def make_request(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

# Verify update by re-fetching the record
def verify_update(catalog_id, subcategory_id):
    data = make_request(f"{BASE_URL}/procurement/catalog/{catalog_id}", headers, {})
    if data and data.get('subcategory', {}).get('id') == subcategory_id:
        return True
    return False

# Define a function to get the catalog ID and retrieve current values
def retrieve_and_update_catalog(product_id, subcategory_id, manufacturer_id, product_type_id, row_data):
    params = {"conditions": f"identifier like '{product_id}%'"}
    data = make_request(f"{BASE_URL}/procurement/catalog", headers, params)
    if data:
        # Extract catalog ID and remove any trailing decimals
        catalog_id = str(int(float(data[0].get('id'))))
        
        # Update with new values from the row data, without the category field
        patch_data = [
            {"op": "replace", "path": "/customFields", "value": [
                {"id": 85, "value": row_data['RenewableItem']},
                {"id": 96, "value": row_data['SpecialOrderItem']}
            ]},
            {"op": "replace", "path": "/subcategory", "value": {"id": subcategory_id}},
            {"op": "replace", "path": "/manufacturer", "value": {"id": manufacturer_id}},
            {"op": "replace", "path": "/type", "value": {"id": product_type_id}},
            {"op": "replace", "path": "/productClass", "value": row_data['Class']},
            {"op": "replace", "path": "/serializedFlag", "value": row_data['Serialized']}
        ]
        
        # Attempt to update the record
        try:
            response = requests.patch(f"{BASE_URL}/procurement/catalog/{catalog_id}", headers=headers, json=patch_data)
            response.raise_for_status()
            if verify_update(catalog_id, subcategory_id):
                print(f"Data has been retrieved and updated for Product ID {product_id} with status {response.status_code}")
                return f"Success ({response.status_code})"
            else:
                print(f"Verification failed for Product ID {product_id}. Expected SubCategoryID {subcategory_id}, but not updated.")
                return "Verification failed"
        except requests.RequestException as e:
            print(f"Update failed for Catalog ID: {catalog_id} - Error: {e}")
            return str(e)
    else:
        print(f"No catalog ID found for ProductID: {product_id}")
        return "Catalog ID not found"

# Define functions to retrieve IDs for each attribute
def get_category_id(category_name):
    params = {"conditions": f"name like '{category_name}%'"}
    data = make_request(f"{BASE_URL}/procurement/categories", headers, params)
    if data:
        return data[0].get('id')
    return None

def get_subcategory_id(category_name, subcategory_name):
    # First, find the category ID based on the category name
    category_id = get_category_id(category_name)
    if category_id:
        # Use both category ID and subcategory name to find the correct subcategory ID
        params = {
            "conditions": f"category/id={category_id} and name like '{subcategory_name}%'"
        }
        data = make_request(f"{BASE_URL}/procurement/subcategories", headers, params)
        if data:
            return data[0].get('id')
    return None

def get_manufacturer_id(manufacturer_name):
    params = {"conditions": f"name like '{manufacturer_name}%'"}
    data = make_request(f"{BASE_URL}/procurement/manufacturers", headers, params)
    if data:
        return data[0].get('id')
    return None

def get_product_type_id(product_type_name):
    params = {"conditions": f"name like '{product_type_name}%'"}
    data = make_request(f"{BASE_URL}/procurement/types", headers, params)
    if data:
        return data[0].get('id')
    return None

# Add new columns to store original and used values
df['OriginalCategory'] = df['category']
df['UsedSubCategoryID'] = None
df['OriginalSubCategory'] = df['subCategory']
df['UsedManufacturerID'] = None
df['OriginalManufacturer'] = df['Manufacturer']
df['UsedProductTypeID'] = None
df['OriginalProductType'] = df['ProductType']

# Apply functions to each ProductID in the DataFrame and retrieve/update records
for index, row in df.iterrows():
    # Retrieve subcategory, manufacturer, and product type IDs using category name and subcategory name
    subcategory_id = get_subcategory_id(row['category'], row['subCategory'])
    manufacturer_id = get_manufacturer_id(row['Manufacturer'])
    product_type_id = get_product_type_id(row['ProductType'])

    # Update DataFrame with the retrieved IDs for logging
    df.at[index, 'UsedSubCategoryID'] = subcategory_id
    df.at[index, 'UsedManufacturerID'] = manufacturer_id
    df.at[index, 'UsedProductTypeID'] = product_type_id

    # Update the catalog entry
    update_status = retrieve_and_update_catalog(
        row['ProductID'], 
        subcategory_id, 
        manufacturer_id, 
        product_type_id, 
        row
    )
    df.at[index, 'UpdateStatus'] = update_status

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file_path, index=False)
print(f"Updated DataFrame saved to {output_file_path}")
