import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}"
}

def get_contacts(company_id):
    """
    Retrieve contacts for a given company ID along with their communication items.
    """
    try:
        params = {
            "conditions": f"(company/id = {company_id})",
            "fields": "id,firstName,lastName,company/name,defaultPhoneType,defaultPhoneNbr,communicationItems"
        }

        response = requests.get(f"{BASE_URL}/company/contacts", headers=headers, params=params)
        response.raise_for_status()

        response_data = response.json()
        if not response_data:
            return pd.DataFrame([{
                "company_id": company_id,
                "contact_id": "No Contacts",
                "first_name": "No Contacts",
                "last_name": "No Contacts",
                "company_name": f"Company_{company_id}",
                "default_phone_type": "No Contacts",
                "default_phone_nbr": "No Contacts",
                "communication_type": "No Contacts",
                "communication_value": "No Contacts"
            }])

        formatted_data = []
        for contact in response_data:
            communication_items = contact.get('communicationItems', [])

            # Extract each communication item and add it to the formatted data
            for item in communication_items:
                comm_type = item.get('type', {}).get('name', 'Unknown')
                comm_value = item.get('value', 'Unknown')

                formatted_data.append({
                    "contact_id": contact.get('id', 'Unknown'),
                    "first_name": contact.get('firstName', 'Unknown'),
                    "last_name": contact.get('lastName', 'Unknown'),
                    "company_id": company_id,
                    "company_name": contact.get('company', {}).get('name', f"Company_{company_id}"),
                    "default_phone_type": contact.get('defaultPhoneType', 'Unknown'),
                    "default_phone_nbr": contact.get('defaultPhoneNbr', 'Unknown'),
                    "communication_type": comm_type,
                    "communication_value": comm_value
                })

        return pd.DataFrame(formatted_data)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching contacts for company ID {company_id}: {e}")
        return pd.DataFrame([{
            "company_id": company_id,
            "contact_id": "Error",
            "first_name": "Error",
            "last_name": "Error",
            "company_name": f"Company_{company_id}",
            "default_phone_type": "Error",
            "default_phone_nbr": "Error",
            "communication_type": "Error",
            "communication_value": "Error"
        }])

def fetch_contacts_from_company_ids(file_path):
    """
    Retrieve contacts for all companies listed in the CSV file.
    """
    if not os.path.isfile(file_path):
        print(f"File '{file_path}' not found.")
        return pd.DataFrame()

    try:
        company_df = pd.read_csv(file_path)
        if 'id' not in company_df.columns:
            print("Error: 'id' column not found in the CSV file.")
            return pd.DataFrame()

        all_contacts = pd.DataFrame()
        for company_id in company_df['id'].dropna().astype(int):
            contacts = get_contacts(company_id)
            all_contacts = pd.concat([all_contacts, contacts], ignore_index=True)

        return all_contacts

    except Exception as e:
        print(f"Error processing company data from file: {e}")
        return pd.DataFrame()

def write_contacts_to_csv(dataframe, file_path):
    """
    Write contact data to a CSV file.
    """
    try:
        dataframe.to_csv(file_path, mode='a', header=not os.path.isfile(file_path), index=False)
        print(f"Contact data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_contact_data():
    """
    Main function to fetch contacts and save them to a CSV file.
    """
    company_file_path = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_Company_Data_101424.csv'
    contacts_df = fetch_contacts_from_company_ids(company_file_path)

    if not contacts_df.empty:
        output_file_path = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_Contact_Data_101424.csv'
        write_contacts_to_csv(contacts_df, output_file_path)
    else:
        print("No contact data to write to CSV.")

# Execute the main function to start the process
upload_contact_data()
