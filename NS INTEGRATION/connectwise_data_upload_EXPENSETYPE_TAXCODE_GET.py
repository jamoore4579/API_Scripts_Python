import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the API endpoint and parameters
endpoint = f"{BASE_URL}/expense/types"
params = {
    "pageSize": 1000,
    "conditions": "inactiveFlag = false"
}

# Send a GET request to the API
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Convert response JSON data to a DataFrame
    work_types = pd.DataFrame(response.json())
    
    # Define the output file path
    output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\TaxCodes\CW_ExpenseType_TaxCode_Output.csv"
    
    # Save data to CSV
    work_types.to_csv(output_file_path, index=False)
    print(f"Data saved to {output_file_path}")
else:
    print(f"Failed to retrieve data: {response.status_code} - {response.text}")
