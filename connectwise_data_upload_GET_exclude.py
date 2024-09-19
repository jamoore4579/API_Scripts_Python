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

def get_users():
    try:
        url = BASE_URL + "/system/members"
        params = {
            "fields": "id,identifier,firstName,lastName,inactiveFlag,requireTimeSheetEntryFlag",
            "pageSize": 1000,
            "conditions": "(requireTimeSheetEntryFlag = false AND inactiveFlag = false)"
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        users = response.json()
        return users
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return []
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return []

def extract_user_data(user_data):
    return {
        "id": user_data.get("id"),
        "identifier": user_data.get("identifier"),
        "firstName": user_data.get("firstName"),
        "lastName": user_data.get("lastName"),
        "requireTimeSheetEntryFlag": user_data.get("requireTimeSheetEntryFlag")
    }

def upload_user_data():
    # Fetch all users
    users = get_users()

    if not users:
        print("No users found.")
        return

    # Create a list to store user results
    user_results = []

    # Iterate through each user
    for user in users:
        user_data = extract_user_data(user)
        user_results.append(user_data)

    # Write user results to CSV file
    user_results_df = pd.DataFrame(user_results)
    user_results_file_path = r'c:\users\jmoore\documents\connectwise\user_data.csv'
    user_results_df.to_csv(user_results_file_path, index=False)
    print(f"User data written to '{user_results_file_path}'.")

# Call the function to load and upload data
upload_user_data()
