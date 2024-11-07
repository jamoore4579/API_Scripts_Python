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

# File paths
input_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_COMPANY_CONTACT.csv"
output_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\CW_Contact_Output_Data_102324.csv"

# Read CSV file
df = pd.read_csv(input_file)

# Ensure CW_ID column exists
if 'CW_ID' not in df.columns:
    raise ValueError("The CSV file does not contain the column 'CW_ID'.")

# Initialize a list to store the API results
contact_data = []

# Iterate over CW_IDs and make API requests
for cw_id in df['CW_ID']:
    try:
        # Construct the API request URL
        api_url = f"{BASE_URL}/company/contacts"
        params = {
            'conditions': f'company/id={cw_id}'
        }

        # Make the API request
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # Check for errors

        # Extract the JSON response
        data = response.json()

        # If no contacts are returned, append "No Contacts" message
        if not data:
            contact_data.append({
                "CW_ID": cw_id,
                "Contact Data": "No Contacts"
            })
        else:
            # Append each contact data for the given CW_ID
            for contact in data:
                contact["CW_ID"] = cw_id  # Add the CW_ID to each contact entry
                contact_data.append(contact)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for CW_ID {cw_id}: {e}")

# Convert the contact data to a DataFrame and save it as a CSV file
if contact_data:
    output_df = pd.DataFrame(contact_data)
    output_df.to_csv(output_file, index=False)
    print(f"Contact data successfully saved to {output_file}")
else:
    print("No contact data retrieved.")
