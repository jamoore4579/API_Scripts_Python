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

# File paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Companies_Updated.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Company_Updates.csv"

# Read the input CSV file
df = pd.read_csv(input_file_path)

# Add columns to store update results
df['Update_Status'] = ''
df['HTTP_Response'] = ''

# Iterate through each row to process the PATCH request
for index, row in df.iterrows():
    company_id = row['CWID']
    netsuite_id = row['ID']

    # Skip if netsuite_id is blank
    if pd.notna(netsuite_id):
        # Prepare PATCH data
        patch_operations = [{
            "op": "replace",
            "path": "/customFields",
            "value": [
                {"id": 60, "value": netsuite_id}
            ]
        }]

        # Make the PATCH request to the ConnectWise API
        try:
            response = requests.patch(
                f"{BASE_URL}/company/companies/{company_id}",
                headers=headers,
                json=patch_operations
            )
            
            # Log the result based on the response
            if response.status_code == 200:
                df.at[index, 'Update_Status'] = 'Success'
            else:
                df.at[index, 'Update_Status'] = 'Failed'
            
            # Store the HTTP response
            df.at[index, 'HTTP_Response'] = response.status_code

        except requests.RequestException as e:
            # Log any exceptions raised during the request
            df.at[index, 'Update_Status'] = 'Error'
            df.at[index, 'HTTP_Response'] = str(e)

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file_path, index=False)

print("Update process completed. Output file created.")
