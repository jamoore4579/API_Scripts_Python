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
        "path": "inactiveFlag",
        "value": True  # Boolean value for the inactiveFlag
    }
]

# Define the file path to read
file_path = r"c:\users\jmoore\documents\connectwise\integration\Product_Inactivated.csv"

# Load the CSV file and read the 'id' column
data = pd.read_csv(file_path)
ids = data['id'].tolist()

# Store results
results = []

# Iterate through each ID and send a PATCH request
for id_value in ids:
    url = f"{BASE_URL}/procurement/catalog/{id_value}"
    try:
        response = requests.patch(url, headers=headers, json=patch_payload)
        # Append result to list
        results.append({
            "id": id_value,
            "status_code": response.status_code,
            "response_text": response.text
        })
    except requests.exceptions.RequestException as e:
        results.append({
            "id": id_value,
            "status_code": "Error",
            "response_text": str(e)
        })

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\patch_results.csv"
results_df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")
