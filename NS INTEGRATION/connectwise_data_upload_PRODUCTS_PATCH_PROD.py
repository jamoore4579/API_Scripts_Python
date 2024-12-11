import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import sys
from lookup_utils import lookup_subcategory, lookup_type, lookup_manufacturer, lookup_uom, lookup_vendor

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
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
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\Activate_Items.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\Patch_Items_Update.csv"

# Define columns to load
columns_to_load = ["ProductID", "category", "subCategory", "uom", "Class", "customerDescription", "notes", "Serialized", "Manufacturer", "ProductType", "RenewableItem", "Price", "PreferredVendor"]

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

# Prepare the results list
results = []

# Iterate through the rows of the DataFrame and send PATCH requests
for index, row in df.iterrows():
    product_id = row["ProductID"]

    # Perform lookups
    subcategory_id = lookup_subcategory(row["subCategory"], BASE_URL, headers)
    type_id = lookup_type(row["ProductType"], BASE_URL, headers)
    manufacturer_id = lookup_manufacturer(row["Manufacturer"], BASE_URL, headers)
    uom_id = lookup_uom(row["uom"], BASE_URL, headers)
    vendor_id = lookup_vendor(row["PreferredVendor"], BASE_URL, headers)

    # Determine the value for "RenewableItem"
    renewable_item_value = True if row["RenewableItem"].strip().lower() == "yes" else False

    # Construct the PATCH payload
    patch_payload = [
        {"op": "replace", "path": "/inactiveFlag", "value": "false"},
        {"op": "replace", "path": "/subcategory", "value": {"id": subcategory_id}},
        {"op": "replace", "path": "/unitOfMeasure", "value": {"id": uom_id}},
        {"op": "replace", "path": "/productClass", "value": row["Class"]},
        {"op": "replace", "path": "/customerDescription", "value": row["customerDescription"]},
        {"op": "replace", "path": "/notes", "value": row["notes"]},
        {"op": "replace", "path": "/serializedFlag", "value": row["Serialized"]},
        {"op": "replace", "path": "/manufacturer", "value": {"id": manufacturer_id}},
        {"op": "replace", "path": "/type", "value": {"id": type_id}},
        {"op": "replace", "path": "/customFields", "value": [{"id": 80, "value": renewable_item_value}]},
        {"op": "replace", "path": "/price", "value": row["Price"]},
        {"op": "replace", "path": "/vendor", "value": {"id": vendor_id}}
    ]

    # Send the PATCH request
    url = f"{BASE_URL}/procurement/catalog/{product_id}"
    try:
        response = requests.patch(url, headers=headers, data=json.dumps(patch_payload))
        results.append({
            "ProductID": product_id,
            "StatusCode": response.status_code,
            "Response": response.json() if response.status_code != 204 else "No Content"
        })
    except requests.RequestException as e:
        results.append({
            "ProductID": product_id,
            "StatusCode": "Error",
            "Response": str(e)
        })

# Save results to an output file
results_df = pd.DataFrame(results)
try:
    results_df.to_csv(output_file_path, index=False)
    print(f"Results written to {output_file_path}")
except Exception as e:
    print(f"Error writing results to output file: {e}")
