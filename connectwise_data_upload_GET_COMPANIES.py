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
        # Initialize an empty list to store all company data
        all_companies = []
        page_number = 1
        
        while True:
            # Parameters for filtering the API response
            params = {
                "pageSize": 1000,
                "page": page_number,
                "conditions": "(deletedFlag = false)"
            }
            
            # Make the API request
            response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors

            # Parse the response data
            response_data = response.json()
            
            # Check if response data is a list
            if isinstance(response_data, list) and response_data:
                all_companies.extend(response_data)  # Add the data to the list
                page_number += 1  # Move to the next page
            else:
                break  # Exit the loop if no more data is available

        # Check if company data is available
        if not all_companies:
            print("No company data found.")
            return None

        # Load company data into a DataFrame
        results_df = pd.DataFrame(all_companies)

        # Handle missing values by replacing them with "Unknown"
        results_df.fillna("Unknown", inplace=True)
        
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
        results_file_path = r'c:\users\jmoore\documents\connectwise\Integration\NS_Integration\company_accounting_data.csv'
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Call the function to load and upload company data
upload_company_data()
