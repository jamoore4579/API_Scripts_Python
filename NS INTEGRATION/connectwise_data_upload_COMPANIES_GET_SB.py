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
                "conditions": "(deletedFlag = false)",
                "customFieldConditions": "caption='Netsuite ID' AND value != null",
                "fields": "id,name,customFields",
                "page": page  # Current page number
            }

            # Make the API request
            response = requests.get(f"{BASE_URL}/company/companies", headers=headers, params=params)
            response.raise_for_status()

            # Parse the JSON response
            response_data = response.json()

            if not isinstance(response_data, list) or not response_data:
                break  # Exit loop if no more data

            # Extract and normalize only required fields
            for company in response_data:
                netsuite_id = None
                if 'customFields' in company and isinstance(company['customFields'], list):
                    for field in company['customFields']:
                        if field.get('caption') == "Netsuite ID":
                            netsuite_id = field.get('value')
                            break

                # Append simplified company data
                all_companies.append({
                    "id": company.get("id"),
                    "name": company.get("name"),
                    "Netsuite ID": netsuite_id
                })

            page += 1  # Move to the next page

        # Convert all data to a DataFrame
        results_df = pd.DataFrame(all_companies)

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
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Company_Opp_Data_112124.csv'
        )
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_company_data()
