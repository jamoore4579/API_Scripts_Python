import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import sys
from lookup_utils import lookup_subcategory

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
columns_to_load = ["ProductID", "category", "subCategory"]

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

# Define a function to process rows
def process_rows(start_index, end_index):
    for index, row in df.iloc[start_index:end_index].iterrows():
        product_id = row["ProductID"]
        category_name = row["category"]
        subcategory_name = row["subCategory"]

        # Perform lookup for subcategory using category and subcategory names
        subcategory_id = lookup_subcategory(category_name, subcategory_name, BASE_URL, headers)

        # Construct the PATCH payload
        patch_payload = []
        if subcategory_id:
            patch_payload.append({"op": "replace", "path": "/subcategory", "value": {"id": subcategory_id}})

        # Skip PATCH if there's nothing to update
        if not patch_payload:
            results.append({
                "ProductID": product_id,
                "StatusCode": "Skipped",
                "Response": "No updates required"
            })
            continue

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

# Process the first batch of five records
process_rows(0, 5)

# Ask the user if they want to continue
user_input = input("Do you want to continue processing the remaining records? (yes/no): ").strip().lower()
if user_input == "yes":
    # Process the remaining records
    process_rows(5, len(df))
else:
    print("Processing stopped by user after the first five records.")

# Save results to an output file
results_df = pd.DataFrame(results)
try:
    results_df.to_csv(output_file_path, index=False)
    print(f"Results written to {output_file_path}")
except Exception as e:
    print(f"Error writing results to output file: {e}")
