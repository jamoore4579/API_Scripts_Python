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

# File path for output
output_file_path = r'c:\users\jmoore\documents\connectwise\integration\ns_integration\items\production\Product_Inventory_OnHand.csv'

# Function to get all inventory pages with conditions
def get_all_inventory():
    inventory_list = []
    page = 1
    while True:
        params = {
            "conditions": "warehouse/id=4 AND onHand>0",
            "page": page
        }
        inventory_url = f"{BASE_URL}/procurement/warehouseBins/6/inventoryOnHand"
        response = requests.get(inventory_url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Failed to retrieve inventory data on page {page}. Status Code: {response.status_code}, Response: {response.text}")
            break
        
        data = response.json()
        if not data:
            break
        
        for item in data:
            item_id = item.get("catalogItem", {}).get("id")
            item_name = item.get("catalogItem", {}).get("identifier")
            on_hand = item.get("onHand")
            cost = None
            total_value = None
            
            # Fetch cost value for each item
            if item_id:
                catalog_url = f"{BASE_URL}/procurement/catalog/{item_id}"
                catalog_response = requests.get(catalog_url, headers=headers)
                if catalog_response.status_code == 200:
                    catalog_data = catalog_response.json()
                    cost = catalog_data.get("cost")
                    
            # Calculate total value
            if on_hand is not None and cost is not None:
                total_value = on_hand * cost
            
            inventory_list.append({
                "ID": item_id,
                "Name": item_name,
                "On Hand": on_hand,
                "Cost": cost,
                "Total Value": total_value
            })
        
        page += 1
    
    return inventory_list

# Get all inventory data
inventory_list = get_all_inventory()

# Convert list to DataFrame and append to CSV
df = pd.DataFrame(inventory_list)
write_header = not os.path.exists(output_file_path) or os.stat(output_file_path).st_size == 0
df.to_csv(output_file_path, mode='a', header=write_header, index=False)
print(f"Inventory data successfully appended to {output_file_path}")
