import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API credentials and base URL from environment variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Load the CSV file containing contact data
csv_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\All_Opportunities_111324.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\All_Opportunities_111324_Results.csv"

# Specify the columns to read from the CSV file, including the new 'Amount' column
columns_to_read = [
    'OpportunityName', 'CompanyID', 'Description', 'BDE', 'Stage', 'PreferredDeliveryMethod-Rep', 'Type',
    'HowManycopiestohanddeliver', 'CloseDate', 'SalePotential', 'ForecastNotes', '470Number', 'DealRegistrationNeeded',
    'InvoiceByDate', 'LeadSource', 'QuotedBy', 'SalesEngineer', 'NetSuiteID', 'Amount', 'OpportunityID',
    'BilledEntityNumber', 'OpportunityPriority'
]

# Load the CSV file, loading only the specified columns if they exist
try:
    contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)
except ValueError as e:
    print(f"Warning: {e}")
    # Fallback to loading all columns and filling missing ones with empty strings
    contacts_df = pd.read_csv(csv_file_path)
    for col in columns_to_read:
        if col not in contacts_df.columns:
            contacts_df[col] = ''  # Add missing columns with empty values

# Handle missing values by replacing NaNs with empty strings
contacts_df = contacts_df.fillna('')

# Function to fetch metadata from the API
def fetch_metadata(endpoint):
    api_url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data from {endpoint}. Status Code: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata: {str(e)}")
        return []

# Function to fetch company data by CompanyID
def fetch_company_data(company_id):
    if not company_id:  # Handle missing or empty CompanyID
        print("CompanyID is missing or empty.")
        return None

    company_api_url = f"{BASE_URL}/company/companies/{company_id}"
    try:
        response = requests.get(company_api_url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()  # Return the company data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching company data for CompanyID {company_id}: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Error occurred while fetching company data for CompanyID {company_id}: {req_err}")
        return None

# Function to fetch contact ID and name for a company from the ConnectWise API
def fetch_contact_info(cw_id):
    contact_api_url = f"{BASE_URL}/company/contacts"
    params = {
        'conditions': f'company/id={cw_id}'
    }
    try:
        response = requests.get(contact_api_url, headers=headers, params=params)
        response.raise_for_status()
        contacts = response.json()

        if contacts:
            # Return the first contact's ID and name
            contact_id = contacts[0]['id']
            contact_name = f"{contacts[0]['firstName']} {contacts[0]['lastName']}"
            return contact_id, contact_name
        else:
            print(f"No contacts found for company ID {cw_id}")
            return None, None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching contacts for company ID {cw_id}: {http_err}")
        return None, None
    except requests.exceptions.RequestException as req_err:
        print(f"Error occurred while fetching contacts for company ID {cw_id}: {req_err}")
        return None, None

# Function to fetch opportunity statuses from the API
def fetch_status_mappings():
    # Fetch the opportunity statuses
    statuses_data = fetch_metadata("/sales/opportunities/statuses")

    # Create a dictionary to map status names to their IDs
    status_dict = {item['name']: item['id'] for item in statuses_data}
    return status_dict

# Function to get field mappings for opportunity types and stages
def get_field_mappings():
    # Fetch opportunity types and stages
    types_data = fetch_metadata("/sales/opportunities/types")
    stages_data = fetch_metadata("/sales/stages")

    # Create dictionaries to map descriptions to IDs
    type_dict = {item['description']: item['id'] for item in types_data}
    stage_dict = {item['name']: item['id'] for item in stages_data}

    return type_dict, stage_dict

# Function to convert a date to ISO 8601 format
def format_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        return pd.to_datetime(date_str).strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None

# Function to map SalePotential to probability
def map_sale_potential_to_probability(sale_potential):
    sale_potential = sale_potential.lower() if sale_potential else ''
    if sale_potential == 'low':
        return 1
    elif sale_potential == 'medium':
        return 3
    elif sale_potential == 'high':
        return 5
    elif sale_potential == 'signed':
        return 9
    else:
        return 1  # If blank or any other value

# Function to map Stage to Status using the fetched status IDs
def map_stage_to_status(stage, status_dict):
    stage = stage.lower() if stage else ''
    if stage == 'won':
        return status_dict.get("Closed Won")
    elif stage == 'closed/lost':
        return status_dict.get("Lost")
    else:
        return status_dict.get("Open")

# Get the type, stage, and status mappings
type_dict, stage_dict = get_field_mappings()
status_dict = fetch_status_mappings()  # Fetch opportunity status mappings

# List to store results
results = []

# Iterate over each row in the DataFrame
for index, row in contacts_df.iterrows():
    # Format the dates or leave them out if blank
    close_date = format_date(row['CloseDate'])
    invoice_by_date = format_date(row['InvoiceByDate'])

    # Map stage and type using the dynamic field mappings
    stage_id = stage_dict.get(row['Stage'], None)
    type_id = type_dict.get(row['Type'], None)

    # Check for missing CompanyID
    if not row['CompanyID']:
        print(f"Skipping record due to missing CompanyID for row {index}")
        continue  # Skip this record if CompanyID is missing

    # Retrieve company data and extract territory information
    company_data = fetch_company_data(row['CompanyID'])
    if company_data:
        territory = company_data['territory'] if 'territory' in company_data else 'Unknown'
        company_name = company_data.get('name', 'Unknown')  # Get the company name
    else:
        territory = 'Unknown'
        company_name = 'Unknown'

    # Fetch contact ID and name for the company
    contact_id, contact_name = fetch_contact_info(row['CompanyID'])

    # Map SalePotential to probability
    probability_value = map_sale_potential_to_probability(row['SalePotential'])

    # Create the probability object as required by the API
    probability = {
        "id": probability_value  # Assuming the API expects the "id" field for probability
    }

    # Map Stage to Status using the fetched status IDs
    status_id = map_stage_to_status(row['Stage'], status_dict)

    # Handle LeadSource, default to 'Other' if blank
    lead_source = row['LeadSource'] if row['LeadSource'] else 'Other'

    # Convert DealRegistrationNeeded to boolean: True if "yes", False if "no" or blank
    deal_reg_needed = True if row['DealRegistrationNeeded'].lower() == 'yes' else False

    # Construct the Salesforce URL using OpportunityID
    salesforce_url = f"https://endeavorcommunications--conectwise.sandbox.lightning.force.com/lightning/r/Opportunity/{row['OpportunityID']}/view"

    # Determine the value for "Quoted By" custom field
    quoted_by_value = row['QuotedBy'] if row['QuotedBy'] else "2024-10-22T00:00:00Z"

    # Set multiple custom fields
    custom_fields = [
        {"id": 8, "caption": "Sales Engineer", "value": row['SalesEngineer']},
        {"id": 18, "caption": "Forecast Notes", "value": row['ForecastNotes']},
        {"id": 21, "caption": "Sales Force", "value": salesforce_url},
        {"id": 25, "caption": "Quoted By", "value": quoted_by_value},
        {"id": 28, "caption": "Territory", "value": territory},
        {"id": 27, "caption": "SF Opportunity ID", "value": row['OpportunityID']},
        {"id": 30, "caption": "Deal Reg Needed", "value": deal_reg_needed},
        {"id": 33, "caption": "Proj Total Contract Amt", "value": str(row['Amount'])},
        {"id": 45, "caption": "Opportunity Category", "value": row['Type']},
        {"id": 51, "caption": "BDE", "value": row['BDE']},
        {"id": 61, "caption": "470 Number", "value": row['470Number']},
        {"id": 67, "caption": "Billed Entity Number", "value": row['BilledEntityNumber']},
        {"id": 73, "caption": "Invoice by Date", "value": invoice_by_date},
        {"id": 75, "caption": "Netsuite ID", "value": row['NetSuiteID']},
        {"id": 76, "caption": "Number of copies deliver?", "value": row['HowManycopiestohanddeliver']},
        {"id": 77, "caption": "Opportunity Priority", "value": row['OpportunityPriority']}
    ]

    # Prepare the payload
    payload = {
        "name": row['OpportunityName'],
        "expectedCloseDate": close_date,
        "type": {"id": type_id},
        "stage": {"id": stage_id},
        "source": lead_source,
        "notes": row['Description'],
        "probability": probability,
        "status": {"id": status_id},
        "company": {"id": row['CompanyID']},
        "contact": {"id": contact_id},
        "primarySalesRep": {"id": 380},
        "secondarySalesRep": {"id": 358},
        "businessUnitId": 10,
        "closedDate": close_date,
        "customFields": custom_fields
    }

    # Remove keys with None values
    payload = {key: value for key, value in payload.items() if value is not None}

    # Build the API URL
    api_url = f"{BASE_URL}/sales/opportunities"

    try:
        # Send the POST request to the ConnectWise API
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            response_data = response.json()
            opportunity_id = response_data.get('id')  # Get the ID from the response
            result = {
                "OpportunityNumber": opportunity_id,  # Save the ID as OpportunityNumber
                "NetSuiteID": row['NetSuiteID'],
                "CompanyName": company_name,
                "CompanyID": row['CompanyID'],
                "ContactName": contact_name,
                "ContactID": contact_id,
                "Status": "Success",
                "ErrorMessage": ""
            }
        else:
            result = {
                "OpportunityNumber": "N/A",
                "NetSuiteID": row['NetSuiteID'],
                "CompanyName": company_name,
                "CompanyID": row['CompanyID'],
                "ContactName": contact_name,
                "ContactID": contact_id,
                "Status": f"Failed: {response.status_code}",
                "ErrorMessage": response.text
            }
    except requests.exceptions.RequestException as e:
        result = {
            "OpportunityNumber": "N/A",
            "NetSuiteID": row['NetSuiteID'],
            "CompanyName": company_name,
            "CompanyID": row['CompanyID'],
            "ContactName": contact_name,
            "ContactID": contact_id,
            "Status": "Error",
            "ErrorMessage": str(e)
        }

    results.append(result)

# Convert the results into a DataFrame
results_df = pd.DataFrame(results)

# Check if output file already exists and append to it if so
if os.path.exists(output_file_path):
    results_df.to_csv(output_file_path, mode='a', index=False, header=False)  # Append without headers
else:
    results_df.to_csv(output_file_path, index=False)  # Write with headers for new file

print(f"Results have been saved to {output_file_path}")
