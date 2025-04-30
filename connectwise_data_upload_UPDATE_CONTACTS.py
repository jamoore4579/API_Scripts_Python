import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Validate environment variables
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing one or more environment variables.")

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# File paths
input_path = r"C:\users\jmoore\documents\connectwise\Company\ContactUpdate.csv"
output_path = r"C:\users\jmoore\documents\connectwise\Company\ContactUpdated.csv"

# Read input file
df_input = pd.read_csv(input_path)

# List to collect matching contacts
matched_contacts = []

# Iterate over each row and query the API
for index, row in df_input.iterrows():
    full_first_name = str(row.get('FirstName', '')).strip()
    full_last_name = str(row.get('LastName', '')).strip()

    # Trim to first segment before space
    first_name = full_first_name.split()[0] if full_first_name else ''
    last_name = full_last_name.split()[0] if full_last_name else ''

    if not first_name or not last_name:
        continue  # Skip if either value is missing

    # Print the trimmed values
    print(f"Searching for: FirstName='{first_name}' | LastName='{last_name}'")

    # Prepare API request
    endpoint = f"{BASE_URL}/company/contacts"
    params = {
        "conditions": f"(firstName like \"{first_name}%\" AND lastName like \"{last_name}%\")",
        "pageSize": 1000
    }

    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        contacts = response.json()
        for contact in contacts:
            matched_contacts.append({
                "InputFirstName": full_first_name,
                "InputLastName": full_last_name,
                "TrimmedFirstName": first_name,
                "TrimmedLastName": last_name,
                "ContactID": contact.get("id"),
                "ContactFirstName": contact.get("firstName"),
                "ContactLastName": contact.get("lastName"),
                "Company": contact.get("company", {}).get("name"),
                "Email": contact.get("email"),
                "Phone": contact.get("phoneNumber")
            })
    else:
        print(f"Failed to fetch contacts for {full_first_name} {full_last_name} (Status: {response.status_code})")

# Save to output file
if matched_contacts:
    df_output = pd.DataFrame(matched_contacts)
    df_output.to_csv(output_path, index=False)
    print(f"\nOutput written to {output_path}")
else:
    print("\nNo matching contacts found.")
