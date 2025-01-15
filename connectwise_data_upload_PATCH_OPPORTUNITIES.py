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

# Path to the CSV file
csv_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\Integration\\NS_Integration\\Opportunity\\Production\\CW_UPDATE.csv"

# Read the CSV file
data = pd.read_csv(csv_file_path)

# Check if the required columns exist
if 'CWOpportunityID' not in data.columns or 'OpportunityID' not in data.columns:
    raise ValueError("The CSV file must contain 'CWOpportunityID' and 'OpportunityID' columns.")

# Function to process records
def process_records(records):
    responses = []
    for index, row in records.iterrows():
        cw_opportunity_id = row['CWOpportunityID']
        opportunity_id = row['OpportunityID']

        # Construct PATCH request data (first JSON)
        patch_data_1 = [
            {
                "op": "replace",
                "path": "/customFields",
                "value": [
                    {
                        "id": 21,
                        "value": f"https://endeavorcommunications.lightning.force.com/lightning/r/Opportunity/{opportunity_id}/view"
                    }
                ]
            }
        ]

        # Construct PATCH request data (second JSON)
        patch_data_2 = [
            {
                "op": "replace",
                "path": "/customFields",
                "value": [
                    {
                        "id": 27,
                        "value": f"{opportunity_id}"
                    }
                ]
            }
        ]

        # Define the API endpoint
        endpoint = f"{BASE_URL}/sales/opportunities/{cw_opportunity_id}"

        # Make the first PATCH request
        response_1 = requests.patch(endpoint, headers=headers, json=patch_data_1)
        print(f"Processed CWOpportunityID: {cw_opportunity_id} (First PATCH)")
        responses.append({"CWOpportunityID": cw_opportunity_id, "Response1": response_1.status_code, "Detail1": response_1.text})

        # Make the second PATCH request
        response_2 = requests.patch(endpoint, headers=headers, json=patch_data_2)
        print(f"Processed CWOpportunityID: {cw_opportunity_id} (Second PATCH)")
        responses.append({"CWOpportunityID": cw_opportunity_id, "Response2": response_2.status_code, "Detail2": response_2.text})
    return responses

# Process the first five records
first_five = data.head(5)
responses = process_records(first_five)

# Output responses for the first five records
for resp in responses:
    print(resp)

# Ask user to proceed with the rest of the records
proceed = input("Do you want to process the remaining records? (yes/no): ").strip().lower()
if proceed == 'yes':
    remaining_records = data.iloc[5:]
    responses = process_records(remaining_records)

    # Output responses for the remaining records
    for resp in responses:
        print(resp)
else:
    print("Processing halted by the user.")
