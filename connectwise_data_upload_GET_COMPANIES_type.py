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
    "Authorization": "Basic " + AUTH_CODE
}

def get_companies():
    try:
        # Parameters for filtering the API response
        params = {
            "pageSize": 1000,
            "conditions": "(deletedFlag = false)",
            "childConditions": "(types/id = 40)",
            "fields": "id,identifier,name,types"  # Only include specified fields
        }
        
        # Make the API request with the new URL
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Check if the response is a list or dictionary
        response_data = response.json()
        if isinstance(response_data, list):
            # If the response is a list, treat it as the company data
            company_data = response_data
        else:
            # Handle unexpected response format
            print("Unexpected response format.")
            return None
        
        # Handle the 'types' field if it's a list or dictionary
        for company in company_data:
            if isinstance(company.get('types'), list):
                company['types'] = ','.join([str(t) for t in company['types']])
            else:
                company['types'] = 'Unknown'

        # Check if company data is available
        if not company_data:
            print("No company data found.")
            return None

        # Load company data into a DataFrame
        results_df = pd.DataFrame(company_data)
        
        # Order columns as specified
        column_order = ["id", "identifier", "name", "types"]
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
        results_file_path = r'c:\users\jmoore\documents\connectwise\projects\company_types_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload company data
upload_company_data()
