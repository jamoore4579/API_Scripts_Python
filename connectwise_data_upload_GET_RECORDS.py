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

def get_agreements():
    try:
        # Parameters for filtering the API response
        params = {
            "conditions": "(cancelledFlag = false)",  # Filter by type id and product ID range
            "fields": "id,name,cancelledFlag,agreementStatus"  # Include specified fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/finance/agreements", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Check if the response is a list or dictionary
        response_data = response.json()
        if isinstance(response_data, list):
            # If the response is a list, treat it as the product data
            agreement_data = response_data
        else:
            # Handle unexpected response format
            print("Unexpected response format.")
            return None

        # Check if product data is available
        if not agreement_data:
            print("No agreement data found.")
            return None

        # Load product data into a DataFrame
        results_df = pd.DataFrame(agreement_data)
        
        # Order columns as specified
        column_order = ["id","name","cancelledFlag","agreementStatus"]
        results_df = results_df.reindex(columns=column_order)
        
        return results_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching agreements from API: {e}")
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
        print(f"Agreement data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_company_data():
    # Get company data from API
    companies_df = get_agreements()

    # If data exists, write to CSV
    if companies_df is not None:
        results_file_path = r'c:\users\jmoore\documents\connectwise\integration\agreement_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload company data
upload_company_data()
