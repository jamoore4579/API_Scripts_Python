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
        # Parameters for the API request without any search conditions
        params = {
            "pageSize": 1000,  # Set a large page size to retrieve more records
            "fields": "id,identifier,name,types,name,status,deletedFlag"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the JSON response data
        response_data = response.json()
        if isinstance(response_data, list):
            company_data = response_data
        else:
            print("Unexpected response format.")
            return None

        # Normalize the nested 'status' and 'types' fields to extract required data
        for company in company_data:
            # Extract 'status_name' from 'status' field
            if isinstance(company.get('status'), dict):
                company['status_name'] = company['status'].get('name', 'Unknown')
            else:
                company['status_name'] = 'Unknown'
            
            # Extract 'types_name' as a comma-separated string of all 'types' names
            if isinstance(company.get('types'), list):
                company['types_name'] = ', '.join([type_item['name'] for type_item in company['types'] if 'name' in type_item])
            else:
                company['types_name'] = 'Unknown'

        # Check if company data is available
        if not company_data:
            print("No company data found.")
            return None

        # Load company data into a DataFrame
        results_df = pd.DataFrame(company_data)
        
        # Order columns as specified
        column_order = ["id", "identifier", "name", "types_name", "status_name", "deletedFlag"]
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

def get_company_name_status_deletedflag_by_id(company_id):
    """
    Retrieves the company name, status, and deletedFlag based on the given company ID using the ConnectWise GET API.
    """
    try:
        # Make the GET request for the specific company ID
        response = requests.get(url=f"{BASE_URL}/company/companies/{company_id}", headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the JSON response to extract company name, status, and deletedFlag
        company_data = response.json()
        company_name = company_data.get("name", "Unknown")
        company_status = company_data.get("status", {}).get("name", "Unknown")
        deleted_flag = company_data.get("deletedFlag", "Unknown")

        return company_name, company_status, deleted_flag

    except requests.exceptions.RequestException as e:
        print(f"Error fetching company {company_id} details from API: {e}")
        return None, None, None

def process_delete_company_data(file_path, output_file):
    """
    Reads the company IDs from the specified file, retrieves each company name, status, and deletedFlag, and saves the results.
    """
    try:
        # Read the existing CSV file with the company IDs
        df = pd.read_csv(file_path)
        
        if 'id' not in df.columns:
            print("The specified file does not contain an 'id' column.")
            return

        # Create a list to hold the results
        results = []

        # Loop through each company ID in the CSV
        for company_id in df['id']:
            company_name, company_status, deleted_flag = get_company_name_status_deletedflag_by_id(company_id)
            results.append({"id": company_id, "company_name": company_name, "status_name": company_status, "deletedFlag": deleted_flag})

        # Create a DataFrame from the results list
        results_df = pd.DataFrame(results)

        # Write results to a new CSV file
        results_df.to_csv(output_file, index=False)
        print(f"Company names, status, and deletedFlag successfully written to '{output_file}'.")

    except Exception as e:
        print(f"Error processing file '{file_path}': {e}")

def upload_company_data():
    # Get company data from API
    companies_df = get_companies()

    # If data exists, write to CSV
    if companies_df is not None:
        results_file_path = r'c:\users\jmoore\documents\connectwise\integration\company_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Path to the CSV file containing company IDs to be processed
delete_data_file_path = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\delete_company_data_results.csv'

# Path to save the results with company names, status, and deletedFlag
results_output_file = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\data_results.csv'

# Process the file and write the results
process_delete_company_data(delete_data_file_path, results_output_file)
