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

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2021.1"
}

# Load the CSV file paths
input_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Inventory\\NSInventoryAddSE.csv"
skipped_items_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Inventory\\NSInventoryAddSE_skipped_items.csv"
combined_output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Inventory\\NSInventoryAddSE_combined_output.csv"
product_details_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Inventory\\NSInventoryAddSE_product_details.csv"

# List of required columns
required_columns = ['ProdID', 'LocationOnHand', 'SerialLotNumber']

# Load input data
# Ensure large numbers are not converted to scientific notation
df = pd.read_csv(input_file_path, dtype={'ProdID': str})

# Filter for required columns
df = df[required_columns]

# Replace NaN values with defaults
df.fillna("", inplace=True)

# Define a function to perform the product lookup
def product_lookup(prod_id):
    try:
        # API request for product lookup
        endpoint = f"{BASE_URL}/procurement/catalog"
        params = {
            "conditions": f"identifier like \"{prod_id}%\"",
            "fields": "id,identifier,description,productClass"
        }
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data and len(data) > 0:
            return data[0]['id'], data[0]['identifier'], data[0].get('productClass', 'Unknown')
        else:
            return None, None, None
    except Exception as e:
        print(f"Error looking up product {prod_id}: {e}")
        return None, None, None

# Create the adjustment details payload
adjustment_details = []
skipped_items = []
product_details = []

for index, row in df.iterrows():
    prod_id = row['ProdID']
    if prod_id:
        product_id, identifier, product_class = product_lookup(prod_id)
        if product_id and identifier:
            product_details.append({
                "ProductID": product_id,
                "Identifier": identifier,
                "ProductClass": product_class
            })
            adjustment_detail = {
                "catalogItem": {
                    "id": product_id,
                    "identifier": identifier
                },
                "description": f"{identifier[:49]}",
                "warehouse": {"id": 3},
                "warehouseBin": {"id": 5},
                "quantityAdjusted": row['LocationOnHand'],
                "adjustment": {
                    "name": "NetsuiteConvert010325SE"
                }
            }
            if row['SerialLotNumber']:
                adjustment_detail["serialNumber"] = f"{row['SerialLotNumber']}"
            adjustment_details.append(adjustment_detail)
        else:
            skipped_items.append({"ProdID": prod_id, "Reason": "Not identified as inventory item"})

# Combine into a single POST payload
payload = {
    "identifier": "NetsuiteConvert010325SE",
    "type": {"id": 3},
    "reason": "Netsuite Conversion",
    "adjustmentDetails": adjustment_details
}

# Perform the POST request
try:
    endpoint = f"{BASE_URL}/procurement/adjustments"
    response = requests.post(endpoint, headers=headers, json=payload)
    if response.status_code == 400:
        print("Error 400: Bad Request. Response details:", response.text)
    elif response.status_code == 500:
        print("Error 500: Internal Server Error. Response details:", response.text)
    else:
        response.raise_for_status()
        print("POST request successful:", response.json())
except requests.exceptions.RequestException as e:
    print("Error in POST request:", e)

# Save combined results to a file
result_summary = {
    "Total Records Processed": len(df),
    "Adjustment Details Count": len(adjustment_details),
    "Skipped Items Count": len(skipped_items)
}
summary_df = pd.DataFrame([result_summary])
summary_df.to_csv(combined_output_file_path, index=False)

# Save skipped items to a separate file
if skipped_items:
    skipped_items_df = pd.DataFrame(skipped_items)
    skipped_items_df.to_csv(skipped_items_file_path, index=False)
    print(f"Skipped items saved to {skipped_items_file_path}")

# Save product details to a separate file
if product_details:
    product_details_df = pd.DataFrame(product_details)
    product_details_df.to_csv(product_details_file_path, index=False)
    print(f"Product details saved to {product_details_file_path}")

print(f"Processing completed. Summary saved to {combined_output_file_path}")
