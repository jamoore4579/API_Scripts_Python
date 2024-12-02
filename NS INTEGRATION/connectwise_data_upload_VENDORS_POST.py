import os
import pandas as pd
import requests
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# File path for input and output CSV
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\All_Vendor_112524.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Updated_Vendors_112524.csv"

# Read CSV file into DataFrame
companies_df = pd.read_csv(input_file_path)

# Fill NaN values with empty strings to avoid JSON errors
companies_df = companies_df.fillna("")

# Add a column for the HTTP result
companies_df["HTTP_Result"] = ""

# Function to create a 30-character or less identifier
def create_identifier(name):
    cleaned_name = re.sub(r'[^A-Za-z0-9 ]+', '', name)
    identifier = cleaned_name.replace(" ", "_")
    return identifier[:30]

# Function to get the billing terms ID
def get_billing_terms_id(term_name):
    try:
        response = requests.get(
            f"{BASE_URL}/finance/billingTerms",
            headers=headers
        )
        if response.status_code == 200:
            terms = response.json()
            for term in terms:
                if term["name"].lower() == term_name.lower():
                    return term["id"]
        for term in terms:
            if term["name"].lower() == "net 30":
                return term["id"]
    except Exception as e:
        print(f"Error fetching billing terms: {e}")
    return None

# Define the function to post each company to ConnectWise API
def add_company_to_connectwise(company_data):
    identifier = create_identifier(company_data.get("NAME", ""))
    term_name = company_data.get("TERM_NAME", "").strip() or "Net 30"
    billing_terms_id = get_billing_terms_id(term_name)

    json_payload = {
        "name": company_data.get("NAME", ""),
        "identifier": identifier,
        "addressLine1": company_data.get("Address", ""),
        "city": company_data.get("City", ""),
        "state": company_data.get("State", ""),
        "zip": company_data.get("ZipCode", ""),
        "country": {"id": 1, "name": "USA"},
        "phoneNumber": company_data.get("Phone", ""),
        "website": "",
        "status": {"id": 1},
        "types": [{"id": 1}, {"id": 6}],
        "site": {"name": "Main"},
        "customFields": [{"id": 82, "value": company_data.get("VENDOR_ID", "")}]
    }

    # Add billing terms ID if available
    if billing_terms_id:
        json_payload["billingTerms"] = {"id": billing_terms_id}

    # Add optional fields only if they have non-empty values
    if "TAX_ID" in company_data:
        json_payload["taxIdentifier"] = company_data.get("TAX_ID", "")
    if "DateAcquired" in company_data:
        json_payload["dateAcquired"] = company_data.get("DateAcquired", "")

    # Include emailAddress only if it is not empty
    email = company_data.get("Email", "").strip()
    if email:  # Add emailAddress only if it's not blank
        json_payload["emailAddress"] = email

    print("Payload being sent:", json_payload)

    response = requests.post(
        f"{BASE_URL}/company/companies",
        headers=headers,
        json=json_payload
    )

    if response.status_code == 201:
        return f"Success: {response.status_code}"
    else:
        return f"Failed: {response.status_code}, {response.text}"

# Iterate through each row in the DataFrame
for index, row in companies_df.iterrows():
    http_result = add_company_to_connectwise(row)
    companies_df.at[index, "HTTP_Result"] = http_result

    if index == 4:  # Pause after processing the first 5 records
        companies_df.to_csv(output_file_path, index=False)  # Save progress
        user_input = input("Processed 5 records. Do you want to continue? (yes/no): ").strip().lower()
        if user_input != "yes":
            print("Stopping script as per user request.")
            break

# Output the updated DataFrame to the new CSV file
companies_df.to_csv(output_file_path, index=False)
