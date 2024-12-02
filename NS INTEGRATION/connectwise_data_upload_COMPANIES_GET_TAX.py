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
    "Authorization": "Basic " + AUTH_CODE
}

def get_companies():
    try:
        all_companies = []  # Store all retrieved companies
        page = 1  # Start with the first page

        while True:
            # Updated parameters, supporting pagination
            params = {
                "childConditions": "(types/id = 50 or types/id = 54 or types/id = 1 or types/id = 34 or types/id = 40 or types/id = 48 or types/id = 53 or types/id = 52 or types/id = 51 or types/id = 49)",
                "conditions": "(deletedFlag = false)",
                "fields": "id,name,status,taxCode",
                "page": page  # Current page number
            }

            # Make the API request
            response = requests.get(f"{BASE_URL}/company/companies", headers=headers, params=params)
            response.raise_for_status()

            # Parse the JSON response
            response_data = response.json()

            if not isinstance(response_data, list) or not response_data:
                break  # Exit loop if no more data

            # Normalize data and extract required fields
            for company in response_data:
                all_companies.append({
                    "id": company.get("id", "Unknown"),
                    "name": company.get("name", "Unknown"),
                    "status_name": company.get("status", {}).get("name", "Unknown"),
                    "tax_code_name": company.get("taxCode", {}).get("name", "Unknown"),
                    "companyEntityType": company.get("companyEntityType", {}).get("name", "Unknown")
                })

            page += 1  # Move to the next page

        # Convert all data to a DataFrame
        results_df = pd.DataFrame(all_companies)

        # Specify desired column order
        column_order = [
            "id", "name", "status_name", "tax_code_name"
        ]

        # Reindex DataFrame to ensure column order
        results_df = results_df.reindex(columns=column_order)

        return results_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching companies from API: {e}")
        return None

def write_companies_to_csv(dataframe, file_path):
    try:
        # Write all data to the specified CSV file, overwriting if the file exists
        dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Company data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_company_data():
    companies_df = get_companies()
    if companies_df is not None:
        results_file_path = (
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Company_Tax_Data_112024.csv'
        )
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_company_data()
