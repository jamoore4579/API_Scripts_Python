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

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2021.1"
}

# Load the CSV file
input_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Items\\RemoveInventoryMW.csv"
df = pd.read_csv(input_file_path)

# Define a function to perform the product lookup
def product_lookup(prod_id):
    try:
        endpoint = f"{BASE_URL}/procurement/catalog"
        params = {
            "conditions": f"identifier=\"{prod_id}\"",
            "fields": "id,identifier,description",
            "pagesize": 1000,
        }
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Debugging output
        print(f"Product lookup response for {prod_id}: {response.status_code}, {response.text}")

        if isinstance(data, list) and len(data) > 0:
            return data[0]['id'], data[0]['identifier'], data[0].get("description", "No description")
        else:
            return None, None, None
    except requests.exceptions.RequestException as e:
        print(f"Error looking up product {prod_id}: {e}")
        return None, None, None

# Function to update inventory adjustment with correct JSON structure
def update_inventory_adjustment(adjustment_details):
    if not adjustment_details:
        print("No products to update.")
        return

    adjustment_id = 150  # Ensure this ID is correct
    update_endpoint = f"{BASE_URL}/procurement/adjustments/{adjustment_id}"
    
    # Construct payload to match required JSON format
    payload = {
        "identifier": "WarehouseMWUpdateV3",
        "type": {"id": 3},
        "reason": "Update MW Warehouse to align with Netsuite V3",
        "adjustmentDetails": adjustment_details
    }

    try:
        print(f"Sending {len(adjustment_details)} records to API...")
        print(f"Payload being sent: {payload}")  # Debugging output
        
        response = requests.put(update_endpoint, headers=headers, json=payload)
        response.raise_for_status()
        
        print(f"Successfully updated {len(adjustment_details)} records!")
        print(f"API Response: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error updating inventory adjustment: {e}")

# Create the adjustment details payload
adjustment_details = []
skipped_items = []

for index, row in df.iterrows():
    rec_id = row.get('RecID')  # Retrieve RecID from input file
    prod_id = row.get('ProdID')
    
    if pd.notna(prod_id):  # Ensure prod_id is valid
        product_id, identifier, description = product_lookup(prod_id)
        
        if product_id and identifier:
            # Ensure description is not longer than 49 characters
            truncated_description = description[:49] if description else "No description"
            if len(description) > 49:
                print(f"Warning: Truncated description for product {prod_id}")

            adjustment_detail = {
                "catalogItem": {
                    "id": product_id,
                    "identifier": identifier
                },
                "description": truncated_description,  # Truncate description to 49 chars
                "warehouse": {"id": 4},  # Ensure correct warehouse ID
                "warehouseBin": {"id": 6},  # Ensure correct warehouse bin ID
                "quantityAdjusted": -1,  # Adjust inventory (ensure API accepts negative values)
                "adjustment": {
                    "name": "WarehouseMWUpdateV3"
                }
            }
            adjustment_details.append(adjustment_detail)
        else:
            skipped_items.append({"RecID": rec_id, "ProdID": prod_id})

# Display skipped items
if skipped_items:
    print("\nSkipped Items (not found in product lookup):")
    print(pd.DataFrame(skipped_items))

# Perform the inventory adjustment update
update_inventory_adjustment(adjustment_details)
