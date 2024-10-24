import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API credentials and base URL from environment variables
BASE_URL = os.getenv("SELL_URL")
AUTH_CODE = os.getenv("AUTH_SSAND")


# Set up headers for the API requests
headers = {
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Load the CSV file containing contact data
csv_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\All_Quotes_102124.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Quotes_Loaded_results.csv"

# Specify the columns to read from the CSV file, including "Connectwise ID" as "company_ID"
columns_to_read = [
    'Document Number', 'CUS ID', 'CUS Name', 'Title', 'Status', 'Primary Contact', 'Email', 'Address',
    'City', 'ZipCode', 'State'
]

# Read the CSV file, loading only the specified columns
contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)

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

# List to store results
results = []
