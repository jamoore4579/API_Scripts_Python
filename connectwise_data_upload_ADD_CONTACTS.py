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

# Load CSV file
file_path = r"c:\users\jmoore\documents\connectwise\integration\update_contact_information.csv"
df = pd.read_csv(file_path)

# Extract the required columns and handle missing values
contact_data = df[['CompanyID', 'FirstName', 'LastName', 'Phone', 'Email']].fillna("N/A")

# Create a list to store the results
results = []

# Iterate through each row and create a contact
for index, row in contact_data.iterrows():
    # Check for invalid or missing values
    if row['CompanyID'] == "N/A" or row['FirstName'] == "N/A" or row['Email'] == "N/A":
        result = f"Skipped contact creation for row {index}. Missing or invalid values: " \
                 f"CompanyID: {row['CompanyID']}, FirstName: {row['FirstName']}, Email: {row['Email']}, Phone: {row['Phone']}."
        print(result)
        results.append({
            "CompanyID": row['CompanyID'],
            "FirstName": row['FirstName'],
            "LastName": row['LastName'],
            "Phone": row['Phone'],
            "Email": row['Email'],
            "ContactID": 'N/A',
            "Result": result
        })
        continue

    # Create the contact JSON data with the specified structure
    contact_json = {
        "firstName": row['FirstName'],
        "lastName": row['LastName'],
        "company": {
            "id": row['CompanyID']
        },
        "communicationItems": [
            {
                "type": {
                    "id": 2,
                    "name": "Direct"
                },
                "value": row['Phone'],
                "communicationType": "Phone"
            },
            {
                "type": {
                    "id": 1,
                    "name": "Email"
                },
                "value": row['Email'],
                "communicationType": "Email"
            }
        ]
    }

    # Construct the endpoint URL
    endpoint_url = f"{BASE_URL}/company/contacts"

    # Make a POST request to create the contact
    response = requests.post(endpoint_url, headers=headers, json=contact_json)

    # Check the response and log the result
    if response.status_code == 201:
        # Parse the response to get the contact ID
        contact_id = response.json().get('id', 'N/A')
        result = f"Contact {row['FirstName']} {row['LastName']} created successfully with Contact ID: {contact_id}. Phone: {row['Phone']}."
    else:
        contact_id = 'N/A'  # Set Contact ID to N/A if the request failed
        result = f"Failed to create contact {row['FirstName']} {row['LastName']}. Error: {response.text}. Phone: {row['Phone']}."

    print(result)
    results.append({
        "CompanyID": row['CompanyID'],
        "FirstName": row['FirstName'],
        "LastName": row['LastName'],
        "Phone": row['Phone'],
        "Email": row['Email'],
        "ContactID": contact_id,  # Add the contact ID to the results
        "Result": result
    })

# Convert results to DataFrame and save to a new CSV file
results_df = pd.DataFrame(results)
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\contact_creation_results.csv"
results_df.to_csv(output_file_path, index=False)
print(f"Results saved to {output_file_path}")
