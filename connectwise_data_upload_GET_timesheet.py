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

def get_time_sheets():
    try:
        params = {
            "conditions": "(dateStart = [2024-5-19T00:00:00Z])",
            "pageSize": 700
        }
        response = requests.get(url=f"{BASE_URL}/time/sheets", headers=headers, params=params)
        response.raise_for_status()
        time_sheets = response.json()
        return time_sheets
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return []
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return []

def extract_time_sheet_data(time_sheet):
    member_info = time_sheet.get("member", {})
    return {
        "id": time_sheet.get("id"),
        "memberId": member_info.get("id"),
        "memberIdentifier": member_info.get("identifier"),
        "memberName": member_info.get("name"),
        "year": time_sheet.get("year"),
        "period": time_sheet.get("period"),
        "dateStart": time_sheet.get("dateStart"),
        "dateEnd": time_sheet.get("dateEnd"),
        "status": time_sheet.get("status"),
        "hours": time_sheet.get("hours")
    }

def upload_time_sheet_data():
    # Fetch time sheets
    time_sheets = get_time_sheets()

    if not time_sheets:
        print("No time sheets found.")
        return

    # Create a list to store results
    results = []

    # Iterate through each time sheet
    for time_sheet in time_sheets:
        data_to_store = extract_time_sheet_data(time_sheet)
        results.append(data_to_store)

    # Write results to CSV file
    results_df = pd.DataFrame(results)
    results_file_path = r'c:\users\jmoore\documents\connectwise\timesheet_data.csv'
    results_df.to_csv(results_file_path, index=False)
    print(f"Time sheet data written to '{results_file_path}'.")

# Call the function to load and upload data
upload_time_sheet_data()
