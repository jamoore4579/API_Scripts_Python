import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Validate environment variables
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing one or more environment variables: BASE_URL, AUTH_CODE, CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Input and output file paths
input_file = r"C:\users\jmoore\documents\connectwise\products\ExtremeItems.csv"
output_file = r"C:\users\jmoore\documents\connectwise\products\ExtremeProductsSerial.csv"

# Read the input CSV containing identifiers
input_df = pd.read_csv(input_file)
identifiers = input_df['identifier'].dropna().unique()

# API endpoint
catalog_endpoint = f"{BASE_URL}/procurement/products"

# List to collect all normalized product data
all_items = []

# Loop through each identifier and query the API
for identifier in identifiers:
    page = 1
    while True:
        params = {
            "conditions": f'(catalogItem/identifier like "{identifier}%")',
            "fields": "id,catalogItem/identifier,salesOrder/id,serialNumberIds",
            "page": page,
            "pageSize": 1000
        }
        response = requests.get(catalog_endpoint, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error for identifier {identifier}: {response.status_code} - {response.text}")
            break

        data = response.json()
        if not data:
            break

        # Normalize: one row per serial number
        for item in data:
            product_id = item.get("id")
            catalog_id = item.get("catalogItem", {}).get("identifier")
            sales_order_id = item.get("salesOrder", {}).get("id")
            serials = item.get("serialNumberIds") or []

            if serials:
                for serial_id in serials:
                    all_items.append({
                        "product_id": product_id,
                        "catalogItem": catalog_id,
                        "salesOrder": sales_order_id,
                        "serialNumberId": serial_id
                    })
            else:
                all_items.append({
                    "product_id": product_id,
                    "catalogItem": catalog_id,
                    "salesOrder": sales_order_id,
                    "serialNumberId": None
                })

        if len(data) < params["pageSize"]:
            break
        page += 1

# Convert to DataFrame and save to CSV
df = pd.DataFrame(all_items)
df.to_csv(output_file, index=False)
print(f"Saved {len(df)} rows to {output_file}")
