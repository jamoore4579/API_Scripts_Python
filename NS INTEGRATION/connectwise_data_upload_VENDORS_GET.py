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
        all_companies = []  # Store all retrieved companies
        page = 1  # Start with the first page

        while True:
            # Updated parameters, supporting pagination
            params = {
                "childConditions": "(types/id = 6)",
                "conditions": "(deletedFlag = false)",
                "fields": "id,identifier,name,status/name,types/name",
                "pageSize": 1000,
                "page": page
            }

            # Make the API request
            response = requests.get(f"{BASE_URL}/company/companies", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Check if data is a list and has content, otherwise exit loop
            if isinstance(data, list):
                if not data:  # Exit loop if the list is empty
                    break
                all_companies.extend(data)
            elif isinstance(data, dict) and "value" in data:
                if not data["value"]:  # Exit loop if "value" key is empty
                    break
                all_companies.extend(data["value"])
            else:
                print("Unexpected data format received from API.")
                break

            page += 1  # Increment page for the next loop iteration

        # Flatten the data to extract nested fields for 'status/name' and 'types/name'
        processed_companies = []
        for company in all_companies:
            # Join all 'types' names with commas
            types_names = ", ".join(type_item.get("name", "") for type_item in company.get("types", []))

            processed_companies.append({
                "id": company.get("id"),
                "identifier": company.get("identifier"),
                "name": company.get("name"),
                "status_name": company.get("status", {}).get("name", ""),
                "types_name": types_names
            })

        # Convert the list of companies to a DataFrame
        return pd.DataFrame(processed_companies, columns=["id", "identifier", "name", "status_name", "types_name"])

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
        # Ensure file path is correctly defined
        results_file_path = (
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Vendor_Data_110624.csv'
        )
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_company_data()
