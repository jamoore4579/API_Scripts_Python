import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Input file path
input_path = r"C:\Users\jmoore\Documents\ConnectWise\company\members_expense_id.csv"

# Load the CSV file
df = pd.read_csv(input_path)

# Ensure 'id' column exists
if 'id' not in df.columns:
    raise ValueError("The CSV file must contain an 'id' column.")

# Define the PATCH payload
patch_payload = [
    {
        "op": "replace",
        "path": "/requireExpenseEntryFlag",
        "value": True
    }
]

# Function to update each member
def update_member(member_id):
    url = f"{BASE_URL}/system/members/{member_id}"
    response = requests.patch(url, headers=headers, json=patch_payload)
    
    if response.status_code == 200:
        return f"Success: Updated member {member_id}"
    else:
        return f"Error {response.status_code}: {response.text}"

# Process each ID in the CSV file
results = [update_member(member_id) for member_id in df['id']]

# Print results
for result in results:
    print(result)
