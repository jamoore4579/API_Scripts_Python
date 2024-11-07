import os
import pandas as pd
import requests
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# File path for input and output CSV
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Add_Vendors_110624.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Updated_Vendors_110624.csv"

# Read CSV file into DataFrame
companies_df = pd.read_csv(input_file_path, usecols=["Name", "Address", "City", "State", "ZipCode", "Phone", "ID"])

# Fill NaN values with empty strings to avoid JSON errors
companies_df = companies_df.fillna("")

# Add a column for the HTTP result
companies_df["HTTP_Result"] = ""

# Function to create a 30-character or less identifier
def create_identifier(name):
    # Remove non-alphanumeric characters and replace spaces with underscores
    cleaned_name = re.sub(r'[^A-Za-z0-9 ]+', '', name)
    identifier = cleaned_name.replace(" ", "_")
    # Truncate to 30 characters if necessary
    return identifier[:30]

# Define the function to post each company to ConnectWise API
def add_company_to_connectwise(company_data):
    identifier = create_identifier(company_data["Name"])
    
    json_payload = {
        "name": company_data["Name"],
        "identifier": identifier,
        "addressLine1": company_data["Address"],
        "city": company_data["City"],
        "state": company_data["State"],
        "zip": company_data["ZipCode"],
        "country": {
            "id": 1,
            "name": "USA"
        },
        "phoneNumber": company_data["Phone"],
        "website": "",
        "status": {
            "name": "not-Approved"
        },
        "types": [
            {
                "id": 6
            }
        ],
        "site": {
            "id": 8451,
            "name": "Main"
        },
        "customFields": [
            {
                "id": 71,
                "value": company_data["ID"]
            }
        ]
    }

    response = requests.post(
        f"{BASE_URL}/company/companies",
        headers=headers,
        json=json_payload
    )

    # Return the HTTP result as a message
    if response.status_code == 201:
        return f"Success: {response.status_code}"
    else:
        return f"Failed: {response.status_code}, {response.text}"

# Iterate through each row in the DataFrame, post data, and save HTTP result
for index, row in companies_df.iterrows():
    http_result = add_company_to_connectwise(row)
    companies_df.at[index, "HTTP_Result"] = http_result

# Output the updated DataFrame to the new CSV file
companies_df.to_csv(output_file_path, index=False)
