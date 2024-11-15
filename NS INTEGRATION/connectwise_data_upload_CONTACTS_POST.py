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

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Load the CSV file paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Contacts_111124.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Contacts_111124_update.csv"

# List of required columns
required_columns = ['ID', 'FirstName', 'LastName', 'Email', 'Phone', 'Role']

# Load input data
df = pd.read_csv(input_file_path)

# Filter for required columns
df = df[required_columns]

# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    netsuite_id = row['ID']
    first_name = row['FirstName']
    last_name = row['LastName']
    email = row['Email']
    phone = row['Phone'] if pd.notna(row['Phone']) else "000-000-0000"  # Default phone number
    role = row['Role'] if pd.notna(row['Role']) else "Primary"

    # Step 1: Lookup the contact type based on the Role, or default to "Primary"
    contact_type_url = f"{BASE_URL}/company/contacts/types"
    contact_type_params = {
        "conditions": f"description like \"{role}%\""
    }
    contact_type_response = requests.get(contact_type_url, headers=headers, params=contact_type_params)
    
    if contact_type_response.status_code == 200:
        contact_types = contact_type_response.json()
        
        # Use the first matching contact type, if available
        if contact_types:
            contact_type_id = contact_types[0]['id']
        else:
            print(f"No matching contact type found for role: {role}")
            continue
    else:
        print(f"Failed to lookup contact type for role {role}: {contact_type_response.text}")
        continue

    # Step 2: Search for the company by Netsuite ID
    company_search_url = f"{BASE_URL}/company/companies"
    company_params = {
        "customFieldConditions": 'caption="Netsuite ID" AND value != null'
    }
    company_response = requests.get(company_search_url, headers=headers, params=company_params)

    if company_response.status_code == 200:
        companies = company_response.json()
        
        # Find company ID by matching Netsuite ID
        company_id = None
        for company in companies:
            if str(company['customFields']['value']) == str(netsuite_id):
                company_id = company['id']
                break

        # If company ID is found, retrieve existing contacts
        if company_id:
            contacts_url = f"{BASE_URL}/company/contacts"
            contact_params = {
                "fields": "company/id,firstName,lastName",
                "conditions": f"company/id={company_id}"
            }
            contacts_response = requests.get(contacts_url, headers=headers, params=contact_params)

            if contacts_response.status_code == 200:
                contacts = contacts_response.json()
                match_found = False

                # Check for existing contact by first and last name
                for contact in contacts:
                    if contact['firstName'] == first_name and contact['lastName'] == last_name:
                        match_found = True
                        break

                # If no match found, create a new contact
                if not match_found:
                    new_contact_url = f"{BASE_URL}/company/contacts"
                    new_contact_data = {
                        "firstName": first_name,
                        "lastName": last_name,
                        "company": {"id": company_id},
                        "communicationItems": [
                            {
                                "type": {"id": 2, "name": "Direct"},
                                "value": phone,
                                "communicationType": "Phone"
                            },
                            {
                                "type": {"id": 1, "name": "Email"},
                                "value": email,
                                "communicationType": "Email"
                            }
                        ],
                        "types": [{"id": contact_type_id}]
                    }

                    create_contact_response = requests.post(new_contact_url, headers=headers, json=new_contact_data)
                    if create_contact_response.status_code == 201:
                        print(f"Contact {first_name} {last_name} created successfully for company ID {company_id}.")
                    else:
                        print(f"Failed to create contact for {first_name} {last_name}: {create_contact_response.text}")
            else:
                print(f"Failed to retrieve contacts for company ID {company_id}: {contacts_response.text}")
        else:
            print(f"No company found with Netsuite ID {netsuite_id}")
    else:
        print(f"Failed to search for companies: {company_response.text}")

# Optionally, save the updated data to a new CSV file
df.to_csv(output_file_path, index=False)
