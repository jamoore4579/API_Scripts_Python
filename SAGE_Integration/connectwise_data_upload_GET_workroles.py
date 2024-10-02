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

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

def get_work_roles():
    try:
        params = {
            "pageSize": 900
        }
        response = requests.get(url=f"{BASE_URL}/time/workRoles", headers=headers, params=params)
        response.raise_for_status()
        work_roles = response.json()
        return work_roles
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return []
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return []

def extract_work_role_data(work_role):
    member_info = work_role.get("member", {})
    return {
        "id": work_role.get("id"),
        "name": work_role.get("name"),
        "inactiveFlag": work_role.get("inactiveFlag"),  # Assuming 'inactiveFlag' is part of 'member'
        "integrationXref": work_role.get("integrationXref")
    }

def upload_work_role_data():
    # Fetch work roles
    work_roles = get_work_roles()

    if not work_roles:
        print("No work roles found.")
        return

    # Create a list to store results
    results = []

    # Iterate through each work role
    for work_role in work_roles:
        data_to_store = extract_work_role_data(work_role)
        results.append(data_to_store)

    # Write results to CSV file
    results_df = pd.DataFrame(results)
    results_file_path = r'c:\users\jmoore\documents\connectwise\work_role_data.csv'
    results_df.to_csv(results_file_path, index=False)
    print(f"Work role data written to '{results_file_path}'.")

# Call the function to load and upload data
upload_work_role_data()
