import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the URL for the API endpoint
url = f"{BASE_URL}/sales/opportunities"

# Load the CSV file
csv_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Delete_Opps_102324.csv"
df = pd.read_csv(csv_file)

# Prepare an empty list to store results
output_results = []

# Loop through the NetsuiteID in the CSV file and make API requests for each one
for netsuite_id in df["NetSuiteID"]:
    # Define the custom field condition using NetsuiteID from the CSV
    params = {
        "customFieldConditions": f'caption="Netsuite ID" AND value={netsuite_id}'
    }
    
    # Make the GET request to the ConnectWise API
    response = requests.get(url, headers=headers, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Iterate through the opportunities in the response
        for opp in data:
            # Extract the relevant fields
            opp_id = opp.get('id')
            custom_fields = opp.get('customFields', [])
            # Filter out the custom fields by id 27 and 70
            custom_field_27 = next((field for field in custom_fields if field['id'] == 27), None)
            custom_field_70 = next((field for field in custom_fields if field['id'] == 70), None)

            # Append the results to the list
            output_results.append({
                'OpportunityID': opp_id,
                'CustomField27': custom_field_27.get('value') if custom_field_27 else None,
                'CustomField70': custom_field_70.get('value') if custom_field_70 else None
            })
    else:
        print(f"Failed to fetch data for NetsuiteID {netsuite_id}. Status code: {response.status_code}")

# Convert the results to a DataFrame
output_df = pd.DataFrame(output_results)

# Define the output file path
output_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_OPP_output_results102324.csv"

# Save the output DataFrame to a CSV file
output_df.to_csv(output_file, index=False)

print(f"Output written to {output_file}")
