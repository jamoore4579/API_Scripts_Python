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

# File paths
input_file_path = r'c:\users\jmoore\documents\connectwise\Products\InventoryOnHandSE.csv'
output_file_path = r'c:\users\jmoore\documents\connectwise\Products\InventoryOnHandOutputSE42125.csv'

# Load ProductIDs
df_input = pd.read_csv(input_file_path)
product_ids = df_input["ProductID"].dropna().unique()

# API endpoint
endpoint = f"{BASE_URL}/procurement/warehouseBins/5/inventoryOnHand"

# Accumulator for results
all_results = []

# Loop through ProductIDs and call the API
for product_id in product_ids:
    params = {
        "conditions": f'catalogItem/identifier like "{product_id}%"',
        "fields": "catalogItem/identifier,warehouse/name,onHand",
        "page": 1,
        "pageSize": 1000
    }

    while True:
        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error for {product_id}: {response.status_code} - {response.text}")
            break

        data = response.json()
        if not data:
            break

        all_results.extend(data)

        if len(data) < 1000:
            break
        params["page"] += 1

# Normalize and export
if all_results:
    df_output = pd.json_normalize(all_results)
    df_output.to_csv(output_file_path, index=False)
    print(f"Exported {len(df_output)} records to {output_file_path}")
else:
    print("No inventory data found.")
