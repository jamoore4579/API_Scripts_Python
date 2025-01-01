import os
import pandas as pd
import requests
from dotenv import load_dotenv
from sales_lookup_utility import lookup_sales_rep, fetch_order_status
from datetime import datetime

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

# File paths for input and output
csv_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\SalesOrders\Netsuite_SalesOrders_121724.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\SalesOrders\Netsuite_SalesOrders_121724_Results.csv"

# Specify columns to read
columns_to_read = [
    'NetsuiteID', 'orderDate', 'salesRep', 'BDE', 'poNumber', 'Notes', 'OrderCategory', 'ReasonClosed', 'Status',
    'CompanyID'
]

# Load the CSV file, handling missing columns gracefully
try:
    contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)
except ValueError as e:
    print(f"Warning: {e}")
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

def format_date_to_iso(date_str):
    """
    Convert date string to ISO 8601 format with 'T' and 'Z'.
    """
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return ''  # Return empty string if invalid date

def post_sales_order(row):
    """
    Send a POST request to create a sales order in ConnectWise.
    """
    formatted_order_date = format_date_to_iso(row['orderDate'])
    payload = {
        "company": {"id": row['CompanyID']},
        "status": {"id": row['Status']},
        "orderDate": formatted_order_date,
        "department": {"id": 23},
        "location": {"id": 2},
        "salesRep": {"id": row['salesRep']},
        "billClosedFlag": False,
        "billShippedFlag": False,
        "notes": row['Notes'],
        "poNumber": row['poNumber'],
        "customFields": [
            {"id": 82, "value": row['NetsuiteID']},
            {"id": 84, "value": row['BDE']},
            {"id": 85, "value": row['OrderCategory']},
            {"id": 86, "value": row['ReasonClosed']}
        ]
    }
    api_url = f"{BASE_URL}/sales/orders"
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        if response.status_code in [200, 201]:
            response_json = response.json()
            created_id = response_json.get('id', 'N/A')  # Extract the 'id' field
            print(f"Record Created Successfully - ID: {created_id}")
            return {"status": "Success", "id": created_id, "response": response_json}
        else:
            print(f"Failed to Create Record - Error: {response.text}")
            return {"status": "Failure", "error": response.text}
    except requests.exceptions.RequestException as e:
        print(f"Error during POST request: {e}")
        return {"status": "Error", "error": str(e)}

# Store results for the output file
results = []

def process_records(start, end):
    """Process records and update results list."""
    for index, row in contacts_df.iloc[start:end].iterrows():
        print(f"Processing row {index + 1}/{len(contacts_df)}")

        # Lookup Sales Rep ID using the lookup_sales_rep utility
        sales_rep_id = lookup_sales_rep(row['salesRep'], BASE_URL, headers)
        if sales_rep_id == 0:
            print(f"SalesRep '{row['salesRep']}' not found. Using default ID 0.")
        row['salesRep'] = sales_rep_id

        # Lookup Status ID using the fetch_order_status utility
        status_data = fetch_order_status(row['Status'], BASE_URL, headers)
        if status_data:
            row['Status'] = status_data.get('id', 0)
        else:
            print(f"Status '{row['Status']}' not found. Using default ID 0.")
            row['Status'] = 0

        # Post sales order
        result = post_sales_order(row)
        result['NetsuiteID'] = row['NetsuiteID']
        results.append(result)

# Process the first 5 records
print("Processing the first 5 records...")
process_records(0, 5)

# Pause to ask user if they want to continue
continue_processing = input("Do you want to continue processing the remaining records? (yes/no): ").strip().lower()
if continue_processing == 'yes':
    print("Continuing with the remaining records...")
    process_records(5, len(contacts_df))
else:
    print("Processing stopped after the first 5 records.")

# Convert results to a DataFrame and save to a CSV file
results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False)

print(f"Processing complete. Results saved to: {output_file_path}")
