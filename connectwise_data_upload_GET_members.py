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

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# API endpoint for system members
endpoint = f"{BASE_URL}/system/members"

# Parameters for API request
params = {
    "conditions": 'inactiveFlag=false AND requireTimeSheetEntryFlag=true',
    "fields": 'id,firstName,lastName,requireExpenseEntryFlag',
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# Making the API request
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Define the output file path
    output_path = r"C:\Users\jmoore\Documents\ConnectWise\company\members_expense.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Member data successfully saved to {output_path}")
else:
    print(f"Failed to retrieve member data. Status Code: {response.status_code}, Response: {response.text}")
