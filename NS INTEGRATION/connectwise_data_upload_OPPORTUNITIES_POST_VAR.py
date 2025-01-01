import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API credentials and base URL from environment variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Load the CSV file containing contact data
csv_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Opportunity\\Production\\Update_Opportunity_122824.csv"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Opportunity\\Production\\All_Opportunities_122824_Results.csv"

# Specify the columns to read from the CSV file
columns_to_read = [
    'name', 'expectedCloseDate', 'stage', 'notes', 'type', 'probability', 'salesRep', 'salesEngineer', 'CW_Company',
    'ForecastCategory', 'ForecastNotes', 'QuotedBy', 'Amount', 'BDE', '470#', '471#', 'frnNumber', 'BEN',
    'InvoiceByDate', 'NetsuiteID'
]

# Load the CSV file, loading only the specified columns if they exist
try:
    contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)
except ValueError as e:
    print(f"Warning: {e}")
    contacts_df = pd.read_csv(csv_file_path)
    for col in columns_to_read:
        if col not in contacts_df.columns:
            contacts_df[col] = ''  # Add missing columns with empty values

# Handle missing values by replacing NaNs with empty strings
contacts_df = contacts_df.fillna('')

# Function to convert a date to ISO 8601 format
def format_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        return pd.to_datetime(date_str).strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None

# Function to extract last name from salesRep input
def extract_last_name(sales_rep):
    if not sales_rep or pd.isna(sales_rep):
        return None
    segments = sales_rep.split()
    if len(segments) == 2:
        return segments[1]  # Use the second segment if there are two
    elif len(segments) == 3:
        return segments[2]  # Use the third segment if there are three
    else:
        return segments[-1]  # Fallback to the last segment

# Function to get primarySalesRep ID from ConnectWise API
def get_primary_sales_rep_id(last_name):
    if not last_name:
        return None
    api_url = f"{BASE_URL}/system/members"
    params = {
        "conditions": f"lastName like \"{last_name}%\""
    }
    try:
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            members = response.json()
            if members:
                return members[0]['id']
            else:
                print(f"No matching member found for last name: {last_name}")
                return None
        else:
            print(f"Failed to fetch member data for last name {last_name}. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching member data for last name {last_name}: {str(e)}")
        return None

# Function to get contact ID using the ConnectWise API
def get_contact_id(company_id):
    if not company_id:
        return None
    api_url = f"{BASE_URL}/company/contacts"
    params = {
        "conditions": f"(company/id = {company_id})"
    }
    try:
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            contacts = response.json()
            if contacts:
                return contacts[0]['id']
            else:
                print(f"No contacts found for company ID: {company_id}")
                return None
        else:
            print(f"Failed to fetch contacts for company ID {company_id}. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching contacts for company ID {company_id}: {str(e)}")
        return None

# List to store results
results = []

# Flag to track if the pause has occurred
pause_completed = False

# Iterate over each row in the DataFrame
for index, row in contacts_df.iterrows():
    if index == 10 and not pause_completed:
        input_response = input("First 10 records processed. Do you want to continue with the rest? (yes/no): ")
        if input_response.strip().lower() != "yes":
            print("Process terminated by the user.")
            break
        pause_completed = True

    # Format the dates
    close_date = format_date(row['expectedCloseDate'])
    invoice_by_date = format_date(row['InvoiceByDate'])
    stage_id = row['stage']
    type_id = row['type']

    # Determine status based on stage value
    if stage_id == 26:
        status = 5
    elif stage_id == 25:
        status = 3
    else:
        status = 1

    # Extract last name from salesRep
    last_name = extract_last_name(row['salesRep'])

    # Lookup primarySalesRep ID, fallback to 'Kilmon' if not found
    primary_sales_rep_id = get_primary_sales_rep_id(last_name)
    if not primary_sales_rep_id:
        primary_sales_rep_id = get_primary_sales_rep_id("Kilmon")

    if not row['CW_Company']:
        print(f"Skipping record due to missing CW_Company for row {index}")
        continue

    contact_id = get_contact_id(row['CW_Company'])

    # Handle probability
    try:
        probability_value = int(float(row['probability']))
    except ValueError:
        probability_value = None

    probability = {"id": probability_value} if probability_value else None

    lead_source = row['ForecastCategory'] if row['ForecastCategory'] else 'Other'

    # Prepare the payload
    payload = {
        "name": row['name'],
        "expectedCloseDate": close_date,
        "type": {"id": type_id},
        "stage": {"id": stage_id},
        "status": {"id": status},
        "source": lead_source,
        "notes": row['notes'],
        "probability": probability,
        "company": {"id": row['CW_Company']},
        "primarySalesRep": {"id": primary_sales_rep_id},
        "secondarySalesRep": {"id": 358},
        "locationId": 2,
        "businessUnitId": 26,
        "contact": {"id": contact_id},
        "customFields": [
            {"id": 25, "caption": "Quoted By", "value": row['QuotedBy'] or "2024-10-22T00:00:00Z"},
            {"id": 8, "caption": "Sales Engineer", "value": row['salesEngineer']},
            {"id": 20, "caption": "Forecast Category", "value": row['ForecastCategory']},
            {"id": 18, "caption": "Forecast Notes", "value": row['ForecastNotes']},
            {"id": 33, "caption": "Proj Total Contract Amt", "value": row['Amount']},
            {"id": 45, "caption": "Opportunity Category", "value": 'New'},
            {"id": 46, "caption": "Customer Type", "value": 'End User'},
            {"id": 51, "caption": "BDE", "value": row['BDE']},
            {"id": 61, "caption": "470 Number", "value": row['470#']},
            {"id": 62, "caption": "471 Number", "value": row['471#']},
            {"id": 71, "caption": "FRN Number", "value": row['frnNumber']},
            {"id": 73, "caption": "Invoice By Date", "value": invoice_by_date},
            {"id": 75, "caption": "NetsuiteID", "value": row['NetsuiteID']},
            {"id": 67, "caption": "Billed Entity Number", "value": row['BEN']}
        ]
    }

    payload = {key: value for key, value in payload.items() if value is not None}

    api_url = f"{BASE_URL}/sales/opportunities"

    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            response_data = response.json()
            opportunity_id = response_data.get('id')
            print(f"Successfully created Opportunity ID: {opportunity_id}")
            result = {
                "OpportunityNumber": opportunity_id,
                "Status": "Success",
                "ErrorMessage": ""
            }
        else:
            result = {
                "OpportunityNumber": "N/A",
                "Status": f"Failed: {response.status_code}",
                "ErrorMessage": response.text
            }
    except requests.exceptions.RequestException as e:
        result = {
            "OpportunityNumber": "N/A",
            "Status": "Error",
            "ErrorMessage": str(e)
        }

    results.append(result)

# Save results
results_df = pd.DataFrame(results)

if os.path.exists(output_file_path):
    results_df.to_csv(output_file_path, mode='a', index=False, header=False)
else:
    results_df.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")
