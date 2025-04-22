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
input_file_path = r'c:\users\jmoore\documents\connectwise\Products\InventoryOnHandOutputMW42125.csv'
output_file_path = r'c:\users\jmoore\documents\connectwise\Products\InventoryOnHandOutputMWcost42125.csv'

# Load ProductIDs
df_input = pd.read_csv(input_file_path)
product_ids = df_input["id"].dropna().unique()

# API endpoint
endpoint = f"{BASE_URL}/procurement/catalog"

# Dictionary to store cost per product ID
cost_lookup = {}

# Loop through ProductIDs and call the API
for product_id in product_ids:
    params = {
        "conditions": f'identifier="{product_id}"',
        "fields": "identifier,cost",
        "page": 1,
        "pageSize": 1
    }

    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error for {product_id}: {response.status_code} - {response.text}")
        continue

    data = response.json()
    if data and isinstance(data, list) and "identifier" in data[0]:
        cost_lookup[product_id] = data[0].get("cost", None)
    else:
        cost_lookup[product_id] = None

# Append cost to original DataFrame
df_input["cost"] = df_input["id"].map(cost_lookup)

# Export to CSV
df_input.to_csv(output_file_path, index=False)
print(f"Exported {len(df_input)} records with cost to {output_file_path}")
