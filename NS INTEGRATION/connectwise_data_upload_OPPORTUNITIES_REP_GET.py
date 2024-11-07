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

# Define the URL and initial parameters with additional conditions for ID range
url = f"{BASE_URL}/sales/opportunities"
params = {
    "fields": 'id,primarySalesRep,stage/name,status/name,customFields',  # Added customFields to retrieve custom field data
    "conditions": 'primarySalesRep/id = 363',  # Added ID range condition
    "pageSize": 1000  # Maximum allowed size
}

# Make the API request
response = requests.get(url, headers=headers, params=params)
response.raise_for_status()  # Raise an error if the request was unsuccessful

# Process the response data with conditional check
data = response.json()
if isinstance(data, dict):
    items = data.get('items', [])
else:
    items = data  # Assume it's a list if not a dict

for item in items:
    # Extract the SF Opportunity ID from custom fields
    custom_fields = item.get('customFields', [])
    sf_opportunity_id = next((field.get('value') for field in custom_fields if field.get('id') == 27), None)
    item['SF_Opportunity_ID'] = sf_opportunity_id  # Add SF Opportunity ID to the data dictionary

# Append the processed data to the list
all_data = pd.DataFrame(items)  # Create DataFrame directly from data list

# Output the DataFrame to a CSV file (append mode)
output_file = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\CW_REP_OPP_output_results_103124.csv'
all_data.to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))

print(f"Data successfully appended to {output_file}. Total records appended: {len(all_data)}")
