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

# Load the CSV file
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Production\billing_terms.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Production\billing_terms_update.csv"

# Read CSV file and include necessary columns
df = pd.read_csv(input_file_path)

# Adding new columns to store the status of each API request
df['API_Status'] = ''  # To capture if the API call was successful or not
df['API_Response'] = ''  # To capture the response message

# Loop through each row and send PATCH requests
for index, row in df.iterrows():
    company_id = row['id']  # Assuming the column 'id' exists in the CSV
    
    # Create the patch operations
    patch_operations = [
        {
            "op": "replace",
            "path": "/billingTerms",
            "value": {"id": 9 }
        }
    ]

    # Construct the API endpoint URL
    url = f"{BASE_URL}/company/companies/{company_id}"

    try:
        # Make the PATCH request
        response = requests.patch(url, headers=headers, json=patch_operations)
        
        # Update the DataFrame with API results
        if response.status_code == 200:
            df.at[index, 'API_Status'] = 'Success'
            df.at[index, 'API_Response'] = response.json()
        else:
            df.at[index, 'API_Status'] = 'Failed'
            df.at[index, 'API_Response'] = response.text

    except Exception as e:
        # Handle exceptions and log them
        df.at[index, 'API_Status'] = 'Error'
        df.at[index, 'API_Response'] = str(e)

# Save the updated DataFrame to the output CSV file
df.to_csv(output_file_path, index=False)

print(f"Updated file saved to {output_file_path}")
