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

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2021.1"
}

# Load the CSV file paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Contacts\NS_Contact_Update.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Contacts\NS_Contacts_update.csv"

# List of required columns
required_columns = ['CW_ID', 'FirstName', 'LastName', 'Email', 'Phone', 'Role']

# Load input data
df = pd.read_csv(input_file_path)

# Filter for required columns
df = df[required_columns]

# Replace NaN values with defaults
df.fillna("", inplace=True)  # Replace NaN with empty strings for text fields

def process_rows(dataframe, start, end):
    """Processes rows in the given range of the DataFrame."""
    for index, row in dataframe.iloc[start:end].iterrows():
        cw_id = row['CW_ID']
        first_name = row['FirstName']
        last_name = row['LastName']
        email = row['Email']
        phone = row['Phone']
        role = row['Role']

        # Step 1: Lookup the contact type based on the Role, or default to "Primary"
        contact_type_url = f"{BASE_URL}/company/contacts/types"
        contact_type_params = {
            "conditions": f"description like \"{role}%\""
        }
        contact_type_response = requests.get(contact_type_url, headers=headers, params=contact_type_params)

        if contact_type_response.status_code == 200:
            contact_types = contact_type_response.json()
            
            # Use the first matching contact type, if available
            contact_type_id = contact_types[0]['id'] if contact_types else None
            if not contact_type_id:
                print(f"No matching contact type found for role: {role}")
                continue
        else:
            print(f"Failed to lookup contact type for role {role}: {contact_type_response.text}")
            continue

        # Step 2: Search for the company using CW_ID
        company_search_url = f"{BASE_URL}/company/companies"
        company_search_params = {
            "conditions": f"id={cw_id}"
        }
        company_response = requests.get(company_search_url, headers=headers, params=company_search_params)

        if company_response.status_code == 200:
            companies = company_response.json()

            if companies:
                company_id = companies[0]['id']

                # Step 3: Create the new contact for the company
                new_contact_url = f"{BASE_URL}/company/contacts"
                new_contact_data = {
                    "company": {"id": company_id},
                    "types": [{"id": contact_type_id}],
                    "firstName": first_name,
                    "lastName": last_name
                }

                # Add communicationItems if email or phone is present
                if email or phone:
                    new_contact_data["communicationItems"] = []
                    if email:
                        new_contact_data["communicationItems"].append({
                            "type": {"id": 1},  # Assuming '1' is the type ID for email
                            "value": email
                        })
                    if phone:
                        new_contact_data["communicationItems"].append({
                            "type": {"id": 2},  # Assuming '2' is the type ID for phone
                            "value": phone
                        })

                # Remove keys with empty values to prevent sending invalid JSON
                new_contact_data = {k: v for k, v in new_contact_data.items() if v}

                create_contact_response = requests.post(new_contact_url, headers=headers, json=new_contact_data)
                if create_contact_response.status_code == 201:
                    print(f"Contact {first_name} {last_name} created successfully for company ID {company_id}.")
                else:
                    print(f"Failed to create contact for {first_name} {last_name}: {create_contact_response.text}")
            else:
                print(f"No company found with CW_ID {cw_id}")
        else:
            print(f"Failed to search for companies: {company_response.text}")

# Process the first 5 rows
process_rows(df, 0, 5)

# Pause for user confirmation
user_input = input("Do you want to continue with the rest of the rows? (yes/no): ").strip().lower()

if user_input == "yes":
    # Process the remaining rows
    process_rows(df, 5, len(df))

# Optionally, save the updated data to a new CSV file
df.to_csv(output_file_path, index=False)
