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
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_Contact_Upload_101224.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Billing_Contact_Update.csv"

# Read CSV with necessary columns
df = pd.read_csv(input_file_path, usecols=['CW_ID', 'NS_ID', 'firstName', 'lastName', 'Email', 'Phone'])

# Function to create a contact using ConnectWise API
def create_contact(contact):
    url = f"{BASE_URL}/company/contacts"

    # Prepare the request data, conditionally adding fields
    data = {
        "firstName": contact['firstName'],
        "company": {"id": contact['CW_ID']}
    }

    # Add last name if it is not empty or NaN
    if pd.notna(contact['lastName']) and contact['lastName'].strip():
        data["lastName"] = contact['lastName']

    # Prepare communication items, adding phone if present
    communication_items = [
        {"type": {"id": 1, "name": "Email"}, "value": contact['Email'], "communicationType": "Email"}
    ]
    if pd.notna(contact['Phone']) and contact['Phone'].strip():
        communication_items.append(
            {"type": {"id": 2, "name": "Direct"}, "value": contact['Phone'], "communicationType": "Phone"}
        )

    data["communicationItems"] = communication_items

    # Make the API call
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 201:
        return response.json().get('id')  # Return the created contact ID
    else:
        print(f"Failed to create contact for {contact['firstName']}: {response.text}")
        return None

# Function to update a contact as a billing contact using PATCH API
def update_billing_contact(company_id, contact_id):
    url = f"{BASE_URL}/company/companies/{company_id}"
    data = [{"op": "replace", "path": "/billingContact", "value": {"id": contact_id}}]
    response = requests.patch(url, json=data, headers=headers)
    if response.status_code == 200:
        print(f"Successfully updated billing contact for company ID {company_id}")
    else:
        print(f"Failed to update billing contact for company ID {company_id}: {response.text}")

# Loop through contacts, create them, and update as billing contacts
results = []

for _, row in df.iterrows():
    contact_id = create_contact(row)
    if contact_id:
        update_billing_contact(row['CW_ID'], contact_id)
        results.append({
            'CW_ID': row['CW_ID'],
            'NS_ID': row['NS_ID'],
            'firstName': row['firstName'],
            'lastName': row['lastName'] if pd.notna(row['lastName']) else '',
            'Email': row['Email'],
            'Phone': row['Phone'] if pd.notna(row['Phone']) else '',
            'ContactID': contact_id
        })

# Save results to output file
results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False)

print(f"Process completed. Results saved to {output_file_path}.")
