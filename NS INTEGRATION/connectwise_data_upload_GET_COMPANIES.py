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
    "Authorization": "Basic " + AUTH_CODE
}

def get_companies():
    try:
        # Updated childConditions to include both 50 and 52 using OR condition
        params = {
            "pageSize": 1000,
            "childConditions": "(types/id = 50 OR types/id = 52)",
            "conditions": "(deletedFlag = false)",
            "fields": "id,identifier,name,types,name,addressLine1,city,state,zip,territory,name,status,deletedFlag,customFields"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the JSON response data
        response_data = response.json()
        
        if not isinstance(response_data, list):
            print("Unexpected response format.")
            return None

        # Normalize the nested fields to extract required data
        for company in response_data:
            # Extract 'status_name' from 'status' field if it exists
            company['status_name'] = company.get('status', {}).get('name', 'Unknown')
            
            # Extract 'types_name' as a comma-separated string of all 'types' names if they exist
            company['types_name'] = ', '.join([type_item['name'] for type_item in company.get('types', []) if 'name' in type_item])
            
            # Extract territory name if it exists
            company['territory_name'] = company.get('territory', {}).get('name', 'Unknown')
            
            # Extract specific custom fields
            if 'customFields' in company and isinstance(company['customFields'], list):
                for custom_field in company['customFields']:
                    field_caption = custom_field.get('caption')
                    field_value = custom_field.get('value')
                    # Only include specific custom fields by their caption name
                    if field_caption in [
                        "Hot Prospect", 
                        "Customer Team Site", 
                        "Primary Channel Partner", 
                        "Secondary Channel Partner", 
                        "vCIO?", 
                        "Netsuite ID", 
                        "Tenant Domain"
                    ]:
                        company[field_caption] = field_value

        # Check if company data is available
        if not response_data:
            print("No company data found.")
            return None

        # Load company data into a DataFrame
        results_df = pd.DataFrame(response_data)
        
        # Define desired column order
        column_order = [
            "id", "identifier", "name", "types_name", "addressLine1", "city", "state", "zip", 
            "territory_name", "status_name", "deletedFlag", "Hot Prospect", "Customer Team Site", 
            "Primary Channel Partner", "Secondary Channel Partner", "vCIO?", "Netsuite ID", "Tenant Domain"
        ]
        
        # Reindex DataFrame based on specified column order, filling with NaNs for missing columns
        results_df = results_df.reindex(columns=column_order)
        
        return results_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching companies from API: {e}")
        return None

def write_companies_to_csv(dataframe, file_path):
    try:
        # Check if the file already exists
        if os.path.isfile(file_path):
            # Append to the CSV file without writing the header
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            # Write to a new CSV file with the header
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Company data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_company_data():
    # Get company data from API
    companies_df = get_companies()

    # If data exists, write to CSV
    if companies_df is not None:
        results_file_path = r'c:\users\jmoore\documents\connectwise\integration\loaded_company_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload company data
upload_company_data()
