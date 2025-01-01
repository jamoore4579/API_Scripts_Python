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
                "conditions": "(deletedFlag=false AND (status/id=1 OR status/id=21 OR status/id=13))",
                "childConditions": "(types/id =50)",
                "fields": "id,name,phoneNumber,customFields,status,website,territory/name,market/name,dateAcquired",
                "page": page  # Current page number
            }

            # Make the API request
            response = requests.get(f"{BASE_URL}/company/companies", headers=headers, params=params)
            response.raise_for_status()

            # Parse the JSON response
            response_data = response.json()

            if not isinstance(response_data, list) or not response_data:
                break  # Exit loop if no more data

            # Extract and normalize nested fields
            for company in response_data:
                # Extract type names as comma-separated string
                company['types_name'] = ', '.join([t['name'] for t in company.get('types', []) if 'name' in t])

                # Extract specific custom fields (Netsuite ID)
                company['Netsuite ID'] = None
                if 'customFields' in company and isinstance(company['customFields'], list):
                    for field in company['customFields']:
                        if field.get('caption') == "Netsuite ID":
                            company['Netsuite ID'] = field.get('value')
                            break

                # Extract billing terms name if available
                company['billingTerms_name'] = company.get('billingTerms', {}).get('name', None)

                # Extract status name if available
                company['status_name'] = company.get('status', {}).get('name', None)

                # Normalize new fields: territory name, market, dateAcquired
                company['territory_name'] = company.get('territory', {}).get('name', None)
                company['market'] = company.get('market', {}).get('name', None)
                company['dateAcquired'] = company.get('dateAcquired', None)

                # Normalize website
                company['website'] = company.get('website', None)

            # Append retrieved companies to the master list
            all_companies.extend(response_data)
            page += 1  # Move to the next page

        # Convert all data to a DataFrame
        results_df = pd.DataFrame(all_companies)

        # Specify desired column order
        column_order = [
            "id", "name", "types_name", "Netsuite ID", "status_name",
            "territory_name", "market", "dateAcquired", "phoneNumber", 
            "website"
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
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Production\CW_Company_Data_122824.csv'
        )
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_company_data()
