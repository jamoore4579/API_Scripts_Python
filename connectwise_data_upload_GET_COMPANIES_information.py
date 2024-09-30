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

# Function to get company details from Connectwise API based on company ID
def get_company_details_by_id(company_id):
    try:
        # Define the fields to be retrieved
        fields = (
            "id,identifier,name,status/name,addressLine1,city,state,zip,country/name,"
            "phoneNumber,website,territory/name,market/name,dateAcquired,billingTerms/name"
        )

        # Make the API request to retrieve company details by ID
        response = requests.get(
            url=f"{BASE_URL}/company/companies/{company_id}",
            headers=headers,
            params={"fields": fields}
        )
        response.raise_for_status()

        print(f"Company details request made to: {response.url} with status code: {response.status_code}")

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching company details for ID {company_id} from API: {e}")
        return None

# Function to get contacts of a company from Connectwise API based on company ID using the base URL
def get_company_contacts(company_id):
    try:
        # Define the fields to be retrieved for contacts, including `id`
        fields = "id,firstName,lastName,defaultPhoneNbr"

        # Use the BASE_URL variable and build the contacts API endpoint
        contacts_url = f"{BASE_URL}/company/contacts"
        params = {
            "conditions": f"(company/id = {company_id})",  # Set the condition to filter by company ID
            "fields": fields
        }

        # Make the API request to retrieve contacts of the company by company ID
        response = requests.get(
            url=contacts_url,
            headers=headers,
            params=params
        )
        response.raise_for_status()

        print(f"Contacts request made to: {response.url} with status code: {response.status_code}")

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching contacts for Company ID {company_id} from API: {e}")
        return None

# Function to format phone numbers: remove leading "1" and keep only the last 10 digits
def format_phone_number(phone_number):
    if phone_number:
        # Remove any leading "1"
        if phone_number.startswith("1"):
            phone_number = phone_number[1:]

        # Keep only the last 10 digits
        phone_number = phone_number[-10:]

    return phone_number

# Function to load only the 'Company_ID' column from the CSV file
def load_customer_data(file_path):
    try:
        # Read only the 'Company_ID' column from the CSV file
        customer_data = pd.read_csv(file_path, usecols=['Company_ID'])
        print(f"Successfully loaded 'Company_ID' column from: {file_path}")
        return customer_data
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

# Function to match customer data with company details and contacts using Company ID
def match_companies_by_id(customers_df):
    matches = []

    for index, customer in customers_df.iterrows():
        company_id = str(customer.get('Company_ID')).strip()  # Ensure the ID is read as a string and trimmed of whitespace

        # Skip if Company_ID is missing or empty
        if not company_id or pd.isnull(company_id):
            matches.append((company_id, "No ID Provided", "No Match Found", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None))
            continue

        # Fetch company details by ID from the API
        company_details = get_company_details_by_id(company_id)

        if company_details:
            # Extract company fields
            identifier = company_details.get('identifier', None)
            name = company_details.get('name', None)
            status_name = company_details.get('status', {}).get('name', None)
            address_line1 = company_details.get('addressLine1', None)
            city = company_details.get('city', None)
            state = company_details.get('state', None)
            zip_code = company_details.get('zip', None)
            country_name = company_details.get('country', {}).get('name', None)
            phone_number = format_phone_number(company_details.get('phoneNumber', None))  # Format the phone number
            website = company_details.get('website', None)
            territory_name = company_details.get('territory', {}).get('name', None)
            market_name = company_details.get('market', {}).get('name', None)
            date_acquired = company_details.get('dateAcquired', None)
            billing_terms_name = company_details.get('billingTerms', {}).get('name', None)

            # Fetch contacts of the company by ID from the API using the updated URL
            contacts = get_company_contacts(company_id)

            # Extract contact fields for each contact and append to the results
            if contacts:
                for contact in contacts:
                    contact_id = contact.get('id', None)  # Extract the contact ID
                    first_name = contact.get('firstName', None)
                    last_name = contact.get('lastName', None)
                    default_phone = format_phone_number(contact.get('defaultPhoneNbr', None))  # Format the contact's phone number

                    # Append all the fields along with contact details to the match list
                    matches.append((
                        company_id, identifier, name, status_name, address_line1, city, state, zip_code,
                        country_name, phone_number, website, territory_name, market_name, date_acquired,
                        billing_terms_name, contact_id, first_name, last_name, default_phone
                    ))
            else:
                # If no contacts are found, log and add a single entry with no contact details
                print(f"No contacts found for Company ID: {company_id}")
                matches.append((
                    company_id, identifier, name, status_name, address_line1, city, state, zip_code,
                    country_name, phone_number, website, territory_name, market_name, date_acquired,
                    billing_terms_name, None, None, None, None
                ))
        else:
            # If no company details found, log and add a single entry with no match found
            print(f"No match found for Company ID: {company_id}")
            matches.append((company_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None))

    # Convert matches to a DataFrame with the required columns
    matched_df = pd.DataFrame(matches, columns=[
        'Company ID', 'Identifier', 'Name', 'Status', 'Address Line1', 'City', 'State', 'Zip', 'Country',
        'Phone Number', 'Website', 'Territory', 'Market', 'Date Acquired', 'Billing Terms',
        'Contact ID', 'Contact First Name', 'Contact Last Name', 'Contact Phone Number'
    ])
    return matched_df

# Function to append the matched data to an existing CSV file
def save_matches_to_csv(matched_df, output_file_path):
    try:
        # If the file exists, append without headers
        if os.path.exists(output_file_path):
            matched_df.to_csv(output_file_path, mode='a', header=False, index=False)
        else:
            # If the file does not exist, create it with headers
            matched_df.to_csv(output_file_path, index=False)

        print(f"Matched data appended to '{output_file_path}'.")
    except Exception as e:
        print(f"Error writing the file: {e}")

# Function to upload the customer data, match against Connectwise companies using ID, and append results to CSV
def upload_and_match_customer_data():
    customer_file_path = r'c:\users\jmoore\documents\connectwise\integration\Company_Information.csv'
    output_file_path = r'c:\users\jmoore\documents\connectwise\integration\Contact_Company_Results.csv'

    # Load only the 'Company_ID' column from the CSV
    customers_df = load_customer_data(customer_file_path)
    if customers_df is None or customers_df.empty:
        print("No customer data to process.")
        return

    # Match the customer data with companies and their contacts using Company ID
    matched_df = match_companies_by_id(customers_df)

    # Append the results to the existing CSV
    save_matches_to_csv(matched_df, output_file_path)

# Call the function to load, match, and save customer data
upload_and_match_customer_data()
