import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Input and output file paths
input_file_path = r"c:\users\jmoore\downloads\Test_Customers.csv"
output_file_path = r"c:\users\jmoore\downloads\Update_Test_Customers.csv"

# Read only the 'id' column from the input CSV
df = pd.read_csv(input_file_path, usecols=['id'])

# Adding new columns to store the status of each API request
df['API_Status'] = ''  # To capture if the API call was successful or not
df['API_Response'] = ''  # To capture the response message

# Iterate through each row in the DataFrame and make API calls
for index, row in df.iterrows():
    # Get the ID for the API request
    company_id = row['id']

    # Skip the row if company_id is blank or missing
    if pd.isna(company_id):
        df.at[index, 'API_Status'] = 'Skipped'
        df.at[index, 'API_Response'] = 'Missing ID'
        print(f"Skipped row {index} due to missing ID.")
        continue

    # Construct the API URL
    api_url = f"{BASE_URL}/company/companies/{company_id}"

    # Prepare the payload
    payload = [
        {
            "op": "replace",
            "path": "/accountNumber",
            "value": ""
        },
        {
            "op": "replace",
            "path": "/taxCode",
            "value": {"id": 65}
        },
        {
            "op": "replace",
            "path": "/types",
            "value": [{"id": 1}]
        },
        {
            "op": "replace",
            "path": "/status",
            "value": {"id": 1}
        }
    ]

    try:
        # Make PATCH request
        response = requests.patch(api_url, headers=headers, json=payload)
        
        # Check response status and update the DataFrame
        if response.status_code == 200:
            df.at[index, 'API_Status'] = 'Success'
            df.at[index, 'API_Response'] = response.text
            print(f"Successfully updated company {company_id}")
        else:
            df.at[index, 'API_Status'] = 'Failed'
            df.at[index, 'API_Response'] = f"Status code: {response.status_code}, Response: {response.text}"
            print(f"Failed to update company {company_id}. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        df.at[index, 'API_Status'] = 'Error'
        df.at[index, 'API_Response'] = str(e)
        print(f"An error occurred while updating company {company_id}: {str(e)}")

# Save the updated DataFrame with the status and response columns to the output file
df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")
