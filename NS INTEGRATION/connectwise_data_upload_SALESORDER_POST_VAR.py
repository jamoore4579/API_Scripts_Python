import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime

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

# File paths for input and output
csv_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\SalesOrders\\Second_Netsuite_SalesOrders_122924.csv"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\SalesOrders\\Second_Netsuite_SalesOrders_122924_Results.csv"

# Specify columns to read
columns_to_read = [
    'NetsuiteID', 'orderDate', 'salesRep', 'BDE', 'poNumber', 'Notes', 'OrderCategory', 'ReasonClosed', 'Status',
    'CompanyID', 'DocumentNumber'
]

# Load the CSV file, handling missing columns gracefully
try:
    contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)
except ValueError:
    contacts_df = pd.read_csv(csv_file_path)
    for col in columns_to_read:
        if col not in contacts_df.columns:
            contacts_df[col] = ''

# Replace NaN values with empty strings
contacts_df = contacts_df.fillna('')

# Function to safely convert CompanyID to integer
def safe_int_conversion(value, default=0):
    try:
        return int(float(value))  # Convert to float first to handle decimals, then to int
    except (ValueError, TypeError):
        return default  # Return default if conversion fails

# Convert CompanyID to integer
contacts_df['CompanyID'] = contacts_df['CompanyID'].apply(lambda x: safe_int_conversion(x))

# Remove decimal points from DocumentNumber
contacts_df['DocumentNumber'] = contacts_df['DocumentNumber'].astype(str).str.split('.').str[0]

def format_date_to_iso(date_str):
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return ''  # Return empty string if invalid date

def fetch_sales_rep_id(sales_rep, base_url, headers):
    sales_rep_segments = sales_rep.split(' ')
    sales_rep_last_name = sales_rep_segments[-1]
    api_url = f"{base_url}/system/members"
    params = {"conditions": f"lastName like '{sales_rep_last_name}%'"}
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data[0]['id'] if data else 0
    return 0

def fetch_opportunity_by_document_number(document_number, base_url, headers):
    api_url = f"{base_url}/sales/opportunities"
    params = {"customFieldConditions": f"id=75 AND value='{document_number}'"}
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data[0] if data else None
    return None

def get_contact_id(company_id):
    if not company_id:
        return None
    api_url = f"{BASE_URL}/company/contacts"
    params = {"conditions": f"(company/id = {company_id})"}
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        contacts = response.json()
        return contacts[0]['id'] if contacts else None
    return None

def post_sales_order(row):
    opportunity = fetch_opportunity_by_document_number(row['DocumentNumber'], BASE_URL, headers)
    opportunity_id = opportunity.get('id') if opportunity else None

    sales_rep_id = fetch_sales_rep_id(row['salesRep'], BASE_URL, headers)
    contact_id = get_contact_id(row['CompanyID'])

    formatted_order_date = format_date_to_iso(row['orderDate'])
    

    # Print IDs to the terminal
    print(f"""
    Status ID: {row['Status']}
    SalesRep ID: {sales_rep_id}
    Company ID: {row['CompanyID']}
    Contact ID: {contact_id}
    
    """)

    payload = {
        "company": {"id": row['CompanyID']},
        "shipToCompany": {"id": row['CompanyID']},
        "billToCompany": {"id": row['CompanyID']},
        "status": {"id": row['Status']},
        "orderDate": formatted_order_date,
        "department": {"id": 23},
        "location": {"id": 2},
        "salesRep": {"id": sales_rep_id} if sales_rep_id else None,
        "contact": {"id": contact_id} if contact_id else None,
        "billClosedFlag": False,
        "billShippedFlag": False,
        "notes": row['Notes'],
        "poNumber": row['poNumber'],
        "opportunity": {"id": opportunity_id} if opportunity_id else None,
        "customFields": [
            {"id": 83, "value": row['NetsuiteID']},
            {"id": 84, "value": row['BDE']},
            {"id": 85, "value": row['OrderCategory']},
            {"id": 86, "value": row['ReasonClosed']}
        ]
    }
    api_url = f"{BASE_URL}/sales/orders"
    response = requests.post(api_url, headers=headers, json=payload)
    if response.status_code in [200, 201]:
        response_json = response.json()
        return {"status": "Success", "id": response_json.get('id', 'N/A')}
    return {"status": "Failure", "error": response.text}

results = []

def process_records(start, end):
    for _, row in contacts_df.iloc[start:end].iterrows():
        result = post_sales_order(row)
        result['NetsuiteID'] = row['NetsuiteID']
        results.append(result)

process_records(0, 5)

continue_processing = input("Do you want to continue processing the remaining records? (yes/no): ").strip().lower()
if continue_processing == 'yes':
    process_records(5, len(contacts_df))

results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False)
