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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load the CSV file paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Billing_Contact.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Billing_Contact_Update.csv"

# Read CSV and include the necessary columns
df = pd.read_csv(input_file_path, usecols=['CW_ID', 'firstName', 'lastName', 'Email', 'Phone'])

# Define a function to send POST requests to the ConnectWise API
def update_billing_contact(row):
    # Skip rows with missing critical information
    if pd.isnull(row['CW_ID']) or pd.isnull(row['firstName']) or pd.isnull(row['lastName']):
        print(f"Skipping row due to missing CW_ID or name information: {row.to_dict()}")
        return None
    
    # Create the JSON payload for the POST request using CW_ID
    payload = {
        "firstName": row['firstName'] if pd.notnull(row['firstName']) else "",
        "lastName": row['lastName'] if pd.notnull(row['lastName']) else "",
        "company": {
            "id": int(row['CW_ID'])  # Convert CW_ID to int, if necessary
        },
        "communicationItems": []
    }

    # Add Phone if it exists
    if pd.notnull(row['Phone']):
        payload["communicationItems"].append({
            "type": {
                "id": 2,
                "name": "Direct"
            },
            "value": str(row['Phone']),
            "communicationType": "Phone"
        })
    
    # Add Email if it exists
    if pd.notnull(row['Email']):
        payload["communicationItems"].append({
            "type": {
                "id": 1,
                "name": "Email"
            },
            "value": row['Email'],
            "communicationType": "Email"
        })

    # Skip sending request if no communication items are available
    if not payload["communicationItems"]:
        print(f"Skipping row due to missing communication items: {row.to_dict()}")
        return None

    # Send the POST request to the ConnectWise API
    url = f"{BASE_URL}/company/contacts"
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get('id')  # Assuming the response returns a contact ID or similar field
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} for row: {row.to_dict()}")
        return None
    except Exception as err:
        print(f"Other error occurred: {err} for row: {row.to_dict()}")
        return None

# Apply the update function to each row and store the results in a new column
df['API_Response_ID'] = df.apply(update_billing_contact, axis=1)

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file_path, index=False)

print(f"Updated file saved to: {output_file_path}")
