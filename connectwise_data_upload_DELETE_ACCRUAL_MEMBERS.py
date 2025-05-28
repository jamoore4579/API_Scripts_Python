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
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Load CSV file
csv_path = r"C:\users\jmoore\documents\connectwise\Company\MemberAccruals052825.csv"
df = pd.read_csv(csv_path)

# Check for required columns
if 'id' not in df.columns or 'member' not in df.columns:
    raise ValueError("CSV must contain 'id' and 'member' columns.")

# Iterate and delete accruals
for index, row in df.iterrows():
    accrual_id = row['id']
    member = row['member']
    url = f"{BASE_URL}/system/members/{member}/accruals/{accrual_id}"
    
    try:
        response = requests.delete(url, headers=headers)
        if response.status_code == 200 or response.status_code == 204:
            print(f"Successfully deleted accrual ID {accrual_id} for member {member}")
        else:
            print(f"Failed to delete accrual ID {accrual_id} for member {member} - Status Code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error deleting accrual ID {accrual_id} for member {member} - {e}")
