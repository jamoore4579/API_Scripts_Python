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
combined_output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Inventory\\NSInventoryAddSE_combined_output.csv"

# List of required columns
required_columns = ['ProdID', 'LocationOnHand', 'SerialLotNumber']

# Load input data
df = pd.read_csv(input_file_path)

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
            "fields": "id,identifier,description"
        }
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data and len(data) > 0:
            return data[0]['id'], data[0]['identifier'], data[0]['description']
        else:
            return None, None, None
    except Exception as e:
        print(f"Error looking up product {prod_id}: {e}")
        return None, None, None

# Define a function to perform the POST request
def post_adjustment():
    http_results = []
    try:
        for index, row in df.iterrows():
            serial_number = row['SerialLotNumber']
            quantity_adjusted = row['LocationOnHand']
            product_id = row['ProductID']
            identifier = row['ProductIdentifier']
            description = row['ProductDescription']
            # Payload for the POST request
            payload = {
                "identifier": "NetsuiteConvert123124SE",
                "type": {"id": 3},
                "reason": "Netsuite Conversion",
                "adjustmentDetails": [
                    {
                        "catalogItem": {
                            "id": product_id,
                            "identifier": identifier
                        },
                        "description": description,
                        "warehouse": {"id": 3},
                        "warehouseBin": {"id": 5},
                        "quantityAdjusted": quantity_adjusted,
                        "serialNumber": serial_number,
                        "adjustment": {
                            "name": "NetsuiteConvert123124SE"
                        }
                    }
                ]
            }
            endpoint = f"{BASE_URL}/inventory/adjustments"
            try:
                response = requests.post(endpoint, headers=headers, json=payload)
                response.raise_for_status()
                http_results.append({"ProdID": row['ProdID'], "Status": "Success", "Response": response.json()})
                print("POST request successful:", response.json())
            except requests.exceptions.RequestException as e:
                http_results.append({"ProdID": row['ProdID'], "Status": "Failed", "Error": str(e)})
                print(f"Error in POST request for ProdID {row['ProdID']}: {e}")
    finally:
        # Save combined results to a file
        http_results_df = pd.DataFrame(http_results)
        combined_df = pd.merge(df, http_results_df, on="ProdID", how="left")
        combined_df.to_csv(combined_output_file_path, index=False)
        print(f"Combined output saved to {combined_output_file_path}")

# Initialize new columns for the API data
df['ProductID'] = None
df['ProductIdentifier'] = None
df['ProductDescription'] = None

# Perform product lookup for each row
for index, row in df.iterrows():
    prod_id = row['ProdID']
    if prod_id:
        product_id, identifier, description = product_lookup(prod_id)
        df.at[index, 'ProductID'] = product_id
        df.at[index, 'ProductIdentifier'] = identifier
        df.at[index, 'ProductDescription'] = description

# Perform the POST request
post_adjustment()

print(f"Processing completed. Results saved to {combined_output_file_path}")
