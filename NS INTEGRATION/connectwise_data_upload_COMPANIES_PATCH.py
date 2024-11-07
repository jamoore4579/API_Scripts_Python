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

# Load the CSV file
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Company.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Company_Update.csv"

# Read CSV file and include necessary columns
df = pd.read_csv(input_file_path)

# Adding new columns to store the status of each API request
df['API_Status'] = ''  # To capture if the API call was successful or not
df['API_Response'] = ''  # To capture the response message

# Define function to create the PATCH request body based on CSV values
def create_patch_request_body(territory_id, market_id, tenant_domain, netsuite_id, revenue_year=None):
    # Create a list to hold the patch operations
    patch_operations = []

    # Add territory if not blank
    if pd.notna(territory_id):
        patch_operations.append({
            "op": "replace",
            "path": "/territory",
            "value": {"id": territory_id}
        })
    
    # Add market if not blank
    if pd.notna(market_id):
        patch_operations.append({
            "op": "replace",
            "path": "/market",
            "value": {"id": market_id}
        })
    
    # Add tenant domain if not blank
    if pd.notna(tenant_domain):
        patch_operations.append({
            "op": "replace",
            "path": "/customFields",
            "value": [
                {"id": 72, "value": tenant_domain}
            ]
        })
    
    # Add Netsuite ID to custom field 71 if not blank
    if pd.notna(netsuite_id):
        patch_operations.append({
            "op": "replace",
            "path": "/customFields",
            "value": [
                {"id": 71, "value": netsuite_id}
            ]
        })

    # If revenue_year is provided, add it to the patch operations
    if revenue_year:
        patch_operations.append({
            "op": "replace",
            "path": "/revenueYear",
            "value": revenue_year
        })

    return patch_operations

# Function to check if revenueYear exists in the company record and set to 2024
def check_and_update_revenue_year(company_id):
    try:
        # Get the current company record
        company_url = f"{BASE_URL}/company/companies/{company_id}"
        response = requests.get(company_url, headers=headers)

        # If successful, check if 'revenueYear' exists in the response
        if response.status_code == 200:
            company_data = response.json()
            return company_data.get('revenueYear') is not None
        else:
            print(f"Failed to retrieve company {company_id} data for revenueYear check. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"An error occurred while retrieving company {company_id} data: {str(e)}")
        return False

# Iterate through each row in the DataFrame and make API calls
for index, row in df.iterrows():
    # Get necessary fields from the CSV for the API request
    company_id = row.get('CW_ID')  # Replace 'CompanyID' with the correct column name that uniquely identifies each company
    tenant_domain = row.get('Tenant Domain', None)
    market_id = row.get('Market', None)
    territory_id = row.get('Territory', None)
    netsuite_id = row.get('ID', None)  # Get the Netsuite ID from the 'ID' column

    # Skip the row if company_id is blank or missing
    if pd.isna(company_id):
        df.at[index, 'API_Status'] = 'Skipped'
        df.at[index, 'API_Response'] = 'Missing Company ID'
        print(f"Skipped row {index} due to missing Company ID.")
        continue

    # Check if revenueYear exists and needs to be updated
    update_revenue_year = check_and_update_revenue_year(company_id)

    # Create request body with revenueYear set to 2024 if it exists
    patch_body = create_patch_request_body(territory_id, market_id, tenant_domain, netsuite_id, 2024 if update_revenue_year else None)
    
    # Skip the API call if there are no valid fields to update
    if not patch_body:
        df.at[index, 'API_Status'] = 'Skipped'
        df.at[index, 'API_Response'] = 'No fields to update'
        print(f"Skipped company {company_id} because no fields to update.")
        continue
    
    # Construct the API URL (replace 'company_id' with the correct value from the relevant column)
    api_url = f"{BASE_URL}/company/companies/{company_id}"

    try:
        # Make PATCH request
        response = requests.patch(api_url, headers=headers, json=patch_body)
        
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
