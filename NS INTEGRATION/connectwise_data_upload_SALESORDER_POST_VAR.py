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

