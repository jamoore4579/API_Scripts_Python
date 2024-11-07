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
                "childConditions": "(types/id = 50)",
                "conditions": "(deletedFlag = false)",
                "fields": ("id,identifier,name,status,addressLine1,city,state,zip,phoneNumber,"
                           "territory,market,accountNumber,defaultContact,taxCode,billToCompany,"
                           "billingSite,billingContact,billingTerms,invoiceToEmailAddress,types,customFields"),
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
                company['status_name'] = company.get('status', {}).get('name', 'Unknown')
                company['territory_name'] = company.get('territory', {}).get('name', 'Unknown')
                company['market_name'] = company.get('market', {}).get('name', 'Unknown')
                company['default_contact_name'] = company.get('defaultContact', {}).get('name', 'Unknown')
                company['tax_code_name'] = company.get('taxCode', {}).get('name', 'Unknown')
                company['bill_to_company_name'] = company.get('billToCompany', {}).get('name', 'Unknown')
                company['billing_site_name'] = company.get('billingSite', {}).get('name', 'Unknown')
                company['billing_contact_name'] = company.get('billingContact', {}).get('name', 'Unknown')
                company['billing_term_name'] = company.get('billingTerms', {}).get('name', 'Unknown')
                company['invoice_to_email'] = company.get('invoiceToEmailAddress', 'Unknown')

                # Extract type names as comma-separated string
                company['types_name'] = ', '.join([t['name'] for t in company.get('types', []) if 'name' in t])

                # Extract specific custom fields (Netsuite ID and Tenant Domain)
                if 'customFields' in company and isinstance(company['customFields'], list):
                    for field in company['customFields']:
                        caption = field.get('caption')
                        value = field.get('value')
                        if caption == "Netsuite ID":
                            company['Netsuite ID'] = value
                        if caption == "Tenant Domain":
                            company['Tenant Domain'] = value

            # Append retrieved companies to the master list
            all_companies.extend(response_data)
            page += 1  # Move to the next page

        # Convert all data to a DataFrame
        results_df = pd.DataFrame(all_companies)

        # Specify desired column order
        column_order = [
            "id", "identifier", "name", "status_name", "addressLine1", "city", "state", "zip",
            "phoneNumber", "territory_name", "market_name", "accountNumber", "default_contact_name",
            "tax_code_name", "bill_to_company_name", "billing_site_name", "billing_contact_name", "billing_term_name",
            "invoice_to_email", "types_name", "deletedFlag", "Netsuite ID", "Tenant Domain"
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
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Company_Data_102924.csv'
        )
        write_companies_to_csv(companies_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_company_data()
