import os
import pandas as pd
import requests
import re
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "clientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.v4+json"  # Specify API version here
}

# Load the CSV file
csv_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Company_111824.csv"
data = pd.read_csv(csv_path)

# Extract the required columns
columns_to_extract = [
    "ID", "name", "address", "city", "state", "zip", "primary_contact", 
    "email", "phone", "web_address", "tenant_domain", 
    "market", "territory", "billing_terms", "date_acquired"
]
filtered_data = data[columns_to_extract]

# Replace NaN values with None for JSON compatibility
filtered_data = filtered_data.applymap(lambda x: None if pd.isna(x) else x)

# Dictionaries to cache location IDs, billing term IDs, and market IDs
location_id_cache = {}
billing_terms_id_cache = {}
market_id_cache = {}

# Function to get location ID based on territory name
def get_location_id(territory_name):
    if territory_name in location_id_cache:
        return location_id_cache[territory_name]
    
    params = {
        "fields": "id",
        "conditions": f"(name like \"{territory_name}%\")"
    }
    response = requests.get(f"{BASE_URL}/system/locations", headers=headers, params=params)
    
    if response.status_code == 200:
        locations = response.json()
        if locations:
            location_id = locations[0].get("id")
            location_id_cache[territory_name] = location_id
            return location_id
    return None

# Function to get billing terms ID based on billing terms name
def get_billing_terms_id(billing_terms_name):
    # Convert "Due on receipt" to "Due Upon Receipt"
    if billing_terms_name == "Due on receipt":
        billing_terms_name = "Due Upon Receipt"

    if billing_terms_name in billing_terms_id_cache:
        return billing_terms_id_cache[billing_terms_name]
    
    params = {
        "fields": "id",
        "conditions": f"(name like \"{billing_terms_name}%\")"
    }
    response = requests.get(f"{BASE_URL}/finance/billingTerms", headers=headers, params=params)
    
    if response.status_code == 200:
        billing_terms = response.json()
        if billing_terms:
            billing_terms_id = billing_terms[0].get("id")
            billing_terms_id_cache[billing_terms_name] = billing_terms_id
            return billing_terms_id
    return None

# Function to get market ID based on market name
def get_market_id(market_name):
    if market_name in market_id_cache:
        return market_id_cache[market_name]
    
    params = {
        "fields": "id",
        "conditions": f"(name like \"{market_name}%\")"
    }
    response = requests.get(f"{BASE_URL}/company/marketDescriptions", headers=headers, params=params)
    
    if response.status_code == 200:
        markets = response.json()
        if markets:
            market_id = markets[0].get("id")
            market_id_cache[market_name] = market_id
            return market_id
    return None

# Function to clean and trim the name field, removing spaces
def clean_name(name):
    # Remove non-alphanumeric characters, then remove spaces, and trim to 26 characters
    return re.sub(r'[^A-Za-z0-9]+', '', name)[:26]

# List to store API results
results = []

# Iterate over each row and allow blank IDs
for _, row in filtered_data.iterrows():
    original_name = row["name"]
    cleaned_name = clean_name(original_name)

    # Retrieve the IDs for location, billing terms, and market
    location_id = get_location_id(row["territory"])
    billing_terms_id = get_billing_terms_id(row["billing_terms"])
    market_id = get_market_id(row["market"])

    # Format the date_acquired field to 'YYYY-MM-DD' format if it exists
    date_acquired = row["date_acquired"]
    if date_acquired:
        try:
            date_acquired = datetime.strptime(str(date_acquired), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            date_acquired = None  # Set to None if date format is invalid

    # Construct the payload with identifier as cleaned_name, name as original_name, and a default site name
    payload = {
        "identifier": cleaned_name,  # Identifier uses cleaned_name with no spaces
        "name": original_name,        # Name uses original name
        "status": { "id": 1 },
        "addressLine1": row["address"],
        "city": row["city"],
        "state": row["state"],
        "zip": row["zip"],
        "country": { "id": 1 },
        "phoneNumber": row["phone"],  # Changed from phone to phoneNumber
        "website": row["web_address"],
        "site": { "name": "Main" },   # Add a default site name
        "customFields": [
            {
                "id": 59,  # tenant_domain custom field ID
                "value": str(row["tenant_domain"])  
            },
            {
                "id": 60,  # ID custom field ID
                "value": str(row["ID"])  
            }
        ],
        "types": [
            {
                "id": 50,
                "name": "EndeavorIT - VAR",
                "_info": {
                    "type_href": "https://psa-sandbox.endeavorit.com/v4_6_release/apis/3.0/company/companies/types/50"
                }
            }
        ]
    }

    # Add dateAcquired if present
    if date_acquired:
        payload["dateAcquired"] = date_acquired

    # Add optional fields if IDs are available
    if market_id is not None:
        payload["market"] = {"id": market_id}
    if location_id is not None:
        payload["territory"] = {"id": location_id}
    if billing_terms_id is not None:
        payload["billingTerms"] = {"id": billing_terms_id}
    
    # Remove any remaining None values from the payload
    payload = {k: v for k, v in payload.items() if v is not None}
    
    # Make the API request to create the company
    response = requests.post(f"{BASE_URL}/company/companies", headers=headers, json=payload)
    
    # Track contact creation details for output
    contact_payload_details = None
    
    if response.status_code == 201:
        print(f"Successfully added company: {original_name}")
        company_id = response.json().get("id")
        
        # Check conditions for creating a contact
        if company_id and (row["primary_contact"] or row["email"] or row["phone"]):
            # Set default contact details if primary_contact is blank
            contact_first_name = "Accounts"
            contact_last_name = "Payable"
            if row["primary_contact"]:
                contact_first_name = row["primary_contact"].split()[0]
                contact_last_name = row["primary_contact"].split()[-1]

            # Construct contact payload
            contact_payload = {
                "firstName": contact_first_name,
                "lastName": contact_last_name,
                "company": {"id": company_id},
                "communicationItems": [],
                "types": [{"id": 10}]
            }
            
            # Add communication items if available
            if row["email"]:
                contact_payload["communicationItems"].append({
                    "type": {"id": 1, "name": "Email"},
                    "value": row["email"],
                    "communicationType": "Email"
                })
            if row["phone"]:
                contact_payload["communicationItems"].append({
                    "type": {"id": 2, "name": "Direct"},
                    "value": row["phone"],
                    "communicationType": "Phone"
                })
            
            # Store contact payload details for output
            contact_payload_details = contact_payload
            
            # Make the API request to create the contact
            contact_response = requests.post(f"{BASE_URL}/company/contacts", headers=headers, json=contact_payload)
            if contact_response.status_code == 201:
                print(f"Successfully added contact for company: {original_name}")
                contact_id = contact_response.json().get("id")
                
                # Update the company to set defaultContact using patch operation with op, path, and value
                if contact_id:
                    update_payload = [
                        {
                            "op": "replace",
                            "path": "/defaultContact",
                            "value": {
                                "id": contact_id
                            }
                        }
                    ]
                    update_response = requests.patch(f"{BASE_URL}/company/companies/{company_id}", headers=headers, json=update_payload)
                    if update_response.status_code == 200:
                        print(f"Successfully set default contact for company: {original_name}")
                    else:
                        print(f"Failed to update default contact for company: {original_name}, Status Code: {update_response.status_code}, Response: {update_response.text}")
            else:
                print(f"Failed to add contact for company: {original_name}, Status Code: {contact_response.status_code}, Response: {contact_response.text}")
    else:
        print(f"Failed to add company: {original_name}, Status Code: {response.status_code}, Payload: {payload}, Response: {response.text}")

    # Log results, including contact payload details if contact was attempted
    result = {
        "name": original_name,
        "status_code": response.status_code,
        "response_text": response.text,
        "contact_payload": contact_payload_details  # Include contact payload details in the output
    }
    results.append(result)

# Convert results to a DataFrame and save to CSV
results_df = pd.DataFrame(results)
output_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_Company_Data_Results_111824.csv"
results_df.to_csv(output_path, index=False)
print(f"Results saved to {output_path}")
