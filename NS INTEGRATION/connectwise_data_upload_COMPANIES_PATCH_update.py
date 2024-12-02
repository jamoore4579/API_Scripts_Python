import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Path to the CSV file
csv_file_path = r"c:\users\jmoore\downloads\update_customers.csv"

# Output file path
output_file_path = r"c:\users\jmoore\downloads\update_results.csv"

# Load the CSV file into a DataFrame
try:
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Error loading CSV file: {e}")
    exit()

# Ensure the ID column exists in the CSV
if 'id' not in df.columns:
    print("Error: 'id' column not found in CSV file.")
    exit()

# Initialize a list to store results
results = []

# Extract IDs and perform the PATCH request for each ID
for company_id in df['id']:
    # Define the payload for the PATCH request
    payload = [
        {
            "op": "replace",
            "path": "/status",
            "value": {"id": 1}
        }
    ]

    # Construct the URL for the PATCH request
    url = f"{BASE_URL}/company/companies/{company_id}"

    try:
        # Make the PATCH request
        response = requests.patch(url, headers=headers, json=payload)
        if response.status_code == 200:
            result_message = "Success"
            print(f"Successfully updated company ID {company_id} to active.")
        else:
            result_message = f"Failed: {response.status_code} - {response.text}"
            print(f"Failed to update company ID {company_id}. Status code: {response.status_code}. Response: {response.text}")
    except Exception as e:
        result_message = f"Error: {e}"
        print(f"Error updating company ID {company_id}: {e}")

    # Append the result to the list
    results.append({"id": company_id, "result": result_message})

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Save the results to a CSV file
try:
    results_df.to_csv(output_file_path, index=False)
    print(f"Results saved to {output_file_path}")
except Exception as e:
    print(f"Error saving results to file: {e}")
