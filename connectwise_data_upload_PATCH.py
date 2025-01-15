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

# Define the JSON payload for the PATCH request
patch_payload = [
    {
        "op": "replace",
        "path": "/serializedCostFlag",
        "value": True
    }
]

# Define the file path to read the input CSV file
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\ns_integration\items\production\SerializedProducts.csv"

# Load the CSV file and extract the 'id' column
data = pd.read_csv(input_file_path)
ids = data['id'].tolist()

# Store results
results = []

# Process the first five records
for id_value in ids[:5]:
    url = f"{BASE_URL}/procurement/catalog/{id_value}"
    try:
        # Send PATCH request
        response = requests.patch(url, headers=headers, json=patch_payload)
        # Append response details to the results list
        results.append({
            "id": id_value,
            "status_code": response.status_code,
            "response_text": response.text
        })
    except requests.exceptions.RequestException as e:
        # Handle exceptions and log the error
        results.append({
            "id": id_value,
            "status_code": "Error",
            "response_text": str(e)
        })

# Pause after the first five records
print("Processed the first five records. Do you want to proceed with the rest? (yes/no)")
user_input = input().strip().lower()

if user_input == "yes":
    # Process the remaining records
    for id_value in ids[5:]:
        url = f"{BASE_URL}/procurement/catalog/{id_value}"
        try:
            # Send PATCH request
            response = requests.patch(url, headers=headers, json=patch_payload)
            # Append response details to the results list
            results.append({
                "id": id_value,
                "status_code": response.status_code,
                "response_text": response.text
            })
        except requests.exceptions.RequestException as e:
            # Handle exceptions and log the error
            results.append({
                "id": id_value,
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
