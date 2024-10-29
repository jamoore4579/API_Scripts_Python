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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load the CSV file paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\TaxCodes\CW_ExpenseType_TaxCode_Output.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\TaxCodes\Updated_CW_ExpenseType_TaxCode.csv"

# Read CSV file and include necessary columns
df = pd.read_csv(input_file_path)

# Prepare an empty list to store the output data
output_data = []

# Loop through each row in the DataFrame to perform the PATCH request
for index, row in df.iterrows():
    expense_type_id = row['id']
    name = row['name']

    # Define the PATCH request payload
    payload = [
        {
            "op": "replace",
            "path": "/integrationXRef",
            "value": "OT010400"
        }
    ]

    # Make the PATCH request
    url = f"{BASE_URL}/expense/types/{expense_type_id}"
    response = requests.patch(url, headers=headers, json=payload)

    # Check for successful response
    if response.status_code == 200:
        updated_data = response.json()
        integration_xref = updated_data.get("integrationXRef", "OT010400")  # Use default if not present in response
        output_data.append({
            "id": expense_type_id,
            "name": name,
            "integrationXRef": integration_xref
        })
    else:
        print(f"Failed to update expense type ID {expense_type_id}. Status Code: {response.status_code}")

# Convert output data to a DataFrame and save it to a new CSV file
output_df = pd.DataFrame(output_data)
output_df.to_csv(output_file_path, index=False)

print("Data updated and saved to:", output_file_path)
