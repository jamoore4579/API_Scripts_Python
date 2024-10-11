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

def get_contacts(company_id):
    """
    Retrieve contacts for a given company ID along with their communication items.

    Parameters:
        company_id (int): The company ID to fetch contacts for.

    Returns:
        DataFrame: A DataFrame containing contact details along with communication items and company name, or a message indicating no contacts.
    """
    try:
        # Parameters for filtering the API response
        params = {
            "conditions": f"(company/id = {company_id})",
            "fields": "id,firstName,lastName,company/name,defaultPhoneType,defaultPhoneNbr,communicationItems/name,communicationItems/email"  # Updated fields
        }

        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/contacts", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse the JSON response data
        response_data = response.json()
        if isinstance(response_data, list):
            contact_data = response_data
        else:
            print(f"Unexpected response format for company ID {company_id}.")
            return pd.DataFrame([{
                "company_id": company_id,
                "company_name": f"Company_{company_id}",  # Placeholder company name
                "contact_id": "No Contacts",
                "first_name": "No Contacts",
                "last_name": "No Contacts",
                "default_phone_type": "No Contacts",
                "default_phone_nbr": "No Contacts",
                "communication_name": "No Contacts",
                "communication_email": "No Contacts"
            }])

        # Check if contact data is available
        if not contact_data:
            # Get the company name from the first response item if available
            company_name = response_data[0].get('company', {}).get('name', f"Company_{company_id}") if len(response_data) > 0 else f"Company_{company_id}"
            # Return a DataFrame indicating no contacts found
            return pd.DataFrame([{
                "company_id": company_id,
                "company_name": company_name,
                "contact_id": "No Contacts",
                "first_name": "No Contacts",
                "last_name": "No Contacts",
                "default_phone_type": "No Contacts",
                "default_phone_nbr": "No Contacts",
                "communication_name": "No Contacts",
                "communication_email": "No Contacts"
            }])

        # Normalize and format the 'communicationItems' field
        formatted_data = []
        company_name = contact_data[0].get('company', {}).get('name', f"Company_{company_id}")  # Extract company name from the first contact
        for contact in contact_data:
            # Extract basic contact information
            contact_id = contact.get('id', 'Unknown')
            first_name = contact.get('firstName', 'Unknown')
            last_name = contact.get('lastName', 'Unknown')
            default_phone_type = contact.get('defaultPhoneType', 'Unknown')
            default_phone_nbr = contact.get('defaultPhoneNbr', 'Unknown')

            # Extract communication items and create separate rows for each contact
            if isinstance(contact.get('communicationItems'), list):
                for item in contact['communicationItems']:
                    comm_name = item.get('name', 'Unknown')
                    comm_email = item.get('email', 'Unknown')

                    # Append formatted data with company name included
                    formatted_data.append({
                        "contact_id": contact_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "company_id": company_id,
                        "company_name": company_name,
                        "default_phone_type": default_phone_type,
                        "default_phone_nbr": default_phone_nbr,
                        "communication_name": comm_name,
                        "communication_email": comm_email
                    })
            else:
                # If no communication items, add a single entry for the contact
                formatted_data.append({
                    "contact_id": contact_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "company_id": company_id,
                    "company_name": company_name,
                    "default_phone_type": default_phone_type,
                    "default_phone_nbr": default_phone_nbr,
                    "communication_name": "Unknown",
                    "communication_email": "Unknown"
                })

        # Load formatted data into a DataFrame
        contacts_df = pd.DataFrame(formatted_data)

        return contacts_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching contacts for company ID {company_id}: {e}")
        # Return a DataFrame indicating an error fetching contacts
        return pd.DataFrame([{
            "company_id": company_id,
            "company_name": f"Company_{company_id}",
            "contact_id": "Error",
            "first_name": "Error",
            "last_name": "Error",
            "default_phone_type": "Error",
            "default_phone_nbr": "Error",
            "communication_name": "Error",
            "communication_email": "Error"
        }])

def fetch_contacts_from_company_ids(file_path):
    """
    Retrieve contacts for all companies listed in the CSV file.

    Parameters:
        file_path (str): The path to the CSV file containing company data.

    Returns:
        DataFrame: A DataFrame containing contacts for all companies listed in the CSV file.
    """
    try:
        # Read the CSV file into a DataFrame
        if os.path.isfile(file_path):
            company_df = pd.read_csv(file_path)
        else:
            print(f"File '{file_path}' not found.")
            return None

        # Retrieve the company IDs from the 'CW_ID' column
        if 'CW_ID' not in company_df.columns:
            print("Error: 'CW_ID' column not found in the CSV file.")
            return None

        company_ids = company_df['CW_ID'].tolist()

        # List to store contact data
        all_contacts_df = pd.DataFrame()

        # Iterate over each company ID and fetch contact data
        for company_id in company_ids:
            contacts_df = get_contacts(company_id)
            if contacts_df is not None:
                all_contacts_df = pd.concat([all_contacts_df, contacts_df], ignore_index=True)

        return all_contacts_df

    except Exception as e:
        print(f"Error processing company data from file: {e}")
        return None

def write_contacts_to_csv(dataframe, file_path):
    """
    Write contact data to a CSV file.

    Parameters:
        dataframe (DataFrame): The DataFrame containing contact data.
        file_path (str): The path to the CSV file for storing contact data.
    """
    try:
        # Check if the file already exists
        if os.path.isfile(file_path):
            # Append to the CSV file without writing the header
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            # Write to a new CSV file with the header
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Contact data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_contact_data():
    """
    Main function to load company IDs from a CSV file and fetch associated contact data.
    """
    # File path to the existing CSV containing contact data
    company_file_path = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\ALL_Contacts.csv'
    
    # Get contact data from company IDs listed in the file
    contacts_df = fetch_contacts_from_company_ids(company_file_path)

    # If data exists, write to CSV
    if contacts_df is not None and not contacts_df.empty:
        results_file_path = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_contact_data.csv'
        write_contacts_to_csv(contacts_df, results_file_path)
    else:
        print("No contact data to write to CSV.")

# Call the function to load and upload contact data
upload_contact_data()
