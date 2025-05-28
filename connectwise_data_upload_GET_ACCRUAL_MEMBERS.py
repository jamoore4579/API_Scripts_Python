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

# Endpoint for listing members
members_endpoint = f"{BASE_URL}/system/members"

# Function to fetch all active members requiring timesheet entry (returning only their IDs)
def get_member_ids():
    member_ids = []
    page = 1
    page_size = 100

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "conditions": "inactiveFlag=false AND requireTimeSheetEntryFlag=true",
            "fields": "id"
        }

        response = requests.get(members_endpoint, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            member_ids.extend([member["id"] for member in data])
            page += 1
        else:
            print(f"Failed to fetch members: {response.status_code} - {response.text}")
            break

    return member_ids

# Function to fetch ALL accruals for a specific member (with pagination)
def get_member_accruals(member_id):
    accruals = []
    page = 1
    page_size = 100
    accruals_endpoint = f"{BASE_URL}/system/members/{member_id}/accruals"

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "fields": "id,accrualType,hours,member"
        }
        response = requests.get(accruals_endpoint, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            # Normalize 'member' field to just the ID
            for entry in data:
                if isinstance(entry.get("member"), dict):
                    entry["member"] = entry["member"].get("id")
            accruals.extend(data)
            page += 1
        else:
            print(f"Failed to fetch accruals for member {member_id}: {response.status_code} - {response.text}")
            break

    return accruals

# Main process
member_ids = get_member_ids()

if member_ids:
    # Gather accruals for all members
    all_accruals = []
    for member_id in member_ids:
        accruals = get_member_accruals(member_id)
        all_accruals.extend(accruals)

    # Save accruals info to CSV
    if all_accruals:
        accruals_df = pd.DataFrame(all_accruals)
        accruals_output_path = r"C:\\users\\jmoore\\documents\\connectwise\\Company\\MemberAccruals052825.csv"
        accruals_df.to_csv(accruals_output_path, index=False)
        print(f"Accruals data saved to {accruals_output_path}")
    else:
        print("No accrual data retrieved.")
else:
    print("No member data retrieved.")