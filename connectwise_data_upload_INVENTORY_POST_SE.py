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
    raise EnvironmentError("Missing one or more environment variables")

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2021.1"
}

# Load CSV
input_file_path = r"c:\users\jmoore\documents\connectwise\Products\InventoryOnHandSE.csv"
df = pd.read_csv(input_file_path)

# Drop rows with missing required values
df = df.dropna(subset=['ProdID', 'Count'])

# Ensure Serial column exists and handle nulls
if 'Serial' in df.columns:
    df['Serial'] = df['Serial'].fillna('')
else:
    df['Serial'] = ''

# Function to get cost, product ID, and trimmed description for a given product identifier
def get_product_cost(prod_id_input):
    endpoint = f"{BASE_URL}/procurement/catalog"
    params = {
        "conditions": f'identifier like "{prod_id_input}%"',
        "fields": "id,cost,description",
        "pageSize": 1
    }
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        items = response.json()
        if items:
            item = items[0]
            cost = item.get("cost", 0.0)
            prod_id = item.get("id", None)
            desc = item.get("description", "")[:49].strip()  # Trim description to 49 characters
            return cost, prod_id, desc
        else:
            print(f"No matching product found for {prod_id_input}")
            return 0.0, None, ""
    else:
        print(f"Failed to fetch product for {prod_id_input}: {response.status_code} - {response.text}")
        return 0.0, None, ""

# Build the list of adjustmentDetails
adjustment_details = []

for index, row in df.iterrows():
    ProdID = str(row['ProdID']).strip()
    Count = int(row['Count'])
    Serial = str(row['Serial']).strip()
    cost, prod_id, desc = get_product_cost(ProdID)

    if prod_id is not None:
        print(f"Adding ProdID: {ProdID}, Count: {Count}, Cost: {cost}, ID: {prod_id}, Desc: {desc}, Serial: {Serial}")
        detail = {
            "catalogItem": {
                "id": prod_id,
                "identifier": ProdID
            },
            "description": desc,
            "quantityOnHand": 0,
            "unitCost": cost,
            "warehouse": {
                "id": 3
            },
            "warehouseBin": {
                "id": 5
            },
            "quantityAdjusted": Count,
            "serialNumber": f"{Serial}",
            "adjustment": {
                "id": 175,
                "name": "WarehouseSEUpdateCostv1"
            }
        }

        # Optional: Include serial if needed and supported
        # detail["serialNumber"] = Serial

        adjustment_details.append(detail)

# Build and send the payload
if adjustment_details:
    payload = {
        "id": 175,
        "identifier": "WarehouseSEUpdateCostv1",
        "type": {
            "id": 3
        },
        "reason": "Updating the cost to the SE inventory.",
        "adjustmentDetails": adjustment_details
    }

    endpoint = f"{BASE_URL}/procurement/adjustments/175"
    response = requests.put(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        print("Successfully submitted bulk adjustment.")
    else:
        print(f"Failed to submit adjustment: {response.status_code} - {response.text}")
else:
    print("No valid adjustment details to submit.")
