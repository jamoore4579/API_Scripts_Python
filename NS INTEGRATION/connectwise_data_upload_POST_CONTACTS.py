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
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\New_Contacts_101424.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Contact_Update_101424.csv"

# List of required columns
required_columns = ['CW_ID', 'firstName', 'lastName', 'Email', 'Phone']

# Read CSV with necessary columns, ignoring missing ones
try:
    df = pd.read_csv(input_file_path, usecols=required_columns)
except ValueError as e:
    print(f"Warning: {e}")
    # Load only available columns
    df = pd.read_csv(input_file_path)
    missing_cols = set(required_columns) - set(df.columns)
    for col in missing_cols:
        df[col] = None  # Add missing columns with None values

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

# Loop through contacts and create them
results = []

for _, row in df.iterrows():
    contact_id = create_contact(row)
    if contact_id:
        results.append({
            'CW_ID': row['CW_ID'],
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
