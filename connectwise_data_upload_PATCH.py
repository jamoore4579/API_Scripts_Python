import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Setup headers for API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the file path to read the input CSV file
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\ns_integration\items\production\UPDATE_COST.csv"

# Load the CSV file and extract 'ID' and 'COST' columns
data = pd.read_csv(input_file_path)
ids_and_costs = data[['ID', 'COST']]

# Store results
results = []

# Process the first five records
for _, row in ids_and_costs.iloc[:5].iterrows():
    id_value = row['ID']
    cost_value = row['COST']
    url = f"{BASE_URL}/procurement/catalog/{id_value}"
    patch_payload = [
        {
            "op": "replace",
            "path": "/cost",
            "value": cost_value
        }
    ]
    try:
        # Send PATCH request
        response = requests.patch(url, headers=headers, json=patch_payload)
        # Append response details to the results list
        results.append({
            "ID": id_value,
            "COST": cost_value,
            "status_code": response.status_code,
            "response_text": response.text
        })
    except requests.exceptions.RequestException as e:
        # Handle exceptions and log the error
        results.append({
            "ID": id_value,
            "COST": cost_value,
            "status_code": "Error",
            "response_text": str(e)
        })

# Pause after the first five records
print("Processed the first five records. Do you want to proceed with the rest? (yes/no)")
user_input = input().strip().lower()

if user_input == "yes":
    # Process the remaining records
    for _, row in ids_and_costs.iloc[5:].iterrows():
        id_value = row['ID']
        cost_value = row['COST']
        url = f"{BASE_URL}/procurement/catalog/{id_value}"
        patch_payload = [
            {
                "op": "replace",
                "path": "/cost",
                "value": cost_value
            }
        ]
        try:
            # Send PATCH request
            response = requests.patch(url, headers=headers, json=patch_payload)
            # Append response details to the results list
            results.append({
                "ID": id_value,
                "COST": cost_value,
                "status_code": response.status_code,
                "response_text": response.text
            })
        except requests.exceptions.RequestException as e:
            # Handle exceptions and log the error
            results.append({
                "ID": id_value,
                "COST": cost_value,
                "status_code": "Error",
                "response_text": str(e)
            })
else:
    print("Operation canceled by the user.")

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\ns_integration\items\production\patch_serial_results.csv"
results_df.to_csv(output_file_path, index=False)

# Print confirmation message
print(f"Results saved to {output_file_path}")
