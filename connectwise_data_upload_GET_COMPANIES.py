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
    "Authorization": "Basic " + AUTH_CODE
}

def get_companies():
    try:
        # Parameters for filtering the API response
        params = {
            "pageSize": 1000,
            "childConditions": "(types/id = 50)",  # Filter by type id and product ID range
            "fields": "id,identifier,name,addressLine1,zip,status,deletedFlag"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Check if the response is a list or dictionary
        response_data = response.json()
        if isinstance(response_data, list):
            # If the response is a list, treat it as the product data
            company_data = response_data
        else:
            # Handle unexpected response format
            print("Unexpected response format.")
            return None
        
        # Normalize the nested 'status' field to extract 'status.name'
        for company in company_data:
            if isinstance(company.get('status'), dict):
                company['status_name'] = company['status'].get('name', 'Unknown')
            else:
                company['status_name'] = 'Unknown'
            
            # Add addressLine1 to the company data
            company['address'] = company.get('addressLine1', 'No Address Provided')

        # Check if product data is available
        if not company_data:
            print("No company data found.")
            return None

        # Load product data into a DataFrame
        results_df = pd.DataFrame(company_data)
        
        # Order columns as specified
        column_order = ["id","identifier","name","address","zip","status_name","deletedFlag"]
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
        results_file_path = r'c:\users\jmoore\documents\connectwise\integration\contact_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload company data
upload_company_data()
