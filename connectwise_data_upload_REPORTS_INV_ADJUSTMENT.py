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

# Adjustment IDs to fetch
adjustment_ids = [111, 113]

# Output path
output_file_path = r'c:\users\jmoore\documents\connectwise\Products\InventoryAdjustments.csv'

# Parameters for filtering
params_template = {
    "fields": "catalogItem/identifier,warehouse/name,quantityAdjusted,serialNumber,adjustment/name",
    "page": 1,
    "pageSize": 1000
}

# Store all data
all_adjustments = []

# Loop through each adjustment ID
for adjustment_id in adjustment_ids:
    params = params_template.copy()
    while True:
        endpoint = f"{BASE_URL}/procurement/adjustments/{adjustment_id}/details"
        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error {response.status_code} on adjustment ID {adjustment_id}: {response.text}")
            break

        try:
            data = response.json()
        except ValueError:
            print(f"Failed to parse JSON for adjustment ID {adjustment_id}")
            break

        if not isinstance(data, list) or not data:
            break

        for item in data:
            all_adjustments.append({
                "Adjustment ID": adjustment_id,
                "Catalog Identifier": item.get("catalogItem", {}).get("identifier"),
                "Warehouse": item.get("warehouse", {}).get("name"),
                "Quantity Adjusted": item.get("quantityAdjusted"),
                "Serial Number": item.get("serialNumber"),
                "Adjustment Name": item.get("adjustment", {}).get("name")
            })

        params["page"] += 1

# Export to CSV
if all_adjustments:
    df = pd.DataFrame(all_adjustments)
    df.to_csv(output_file_path, index=False)
    print(f"Exported {len(df)} records from {adjustment_ids} to {output_file_path}")
else:
    print("No adjustment data found.")
