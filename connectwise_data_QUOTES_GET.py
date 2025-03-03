import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API credentials and base URL from environment variables
SELL_URL = os.getenv("SELL_URL")
AUTH_ID = os.getenv("AUTH_ID")


# Set up headers for the API requests
headers = {
    "Authorization": f"Basic {AUTH_ID}",
    "Content-Type": "application/json"
}

# CSV file containing quote data
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Quotes_Loaded_results.csv"

# Specify the columns to read from the CSV file, including "Connectwise ID" as "company_ID"
columns_to_read = [
    'Document Number', 'CUS ID', 'CUS Name', 'Title', 'Status', 'Primary Contact', 'Email', 'Address',
    'City', 'ZipCode', 'State'
]


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
