import os
import pandas as pd
import requests
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

def load_and_upload_accruals():

    # Ask the user for the CSV file path
    file_name = input("Enter the relative path to the CSV file: ")

    # Get the current directory
    base_path = os.getcwd()

    # Construct the full file path
    csv_file_path = os.path.abspath(os.path.join(base_path, file_name))

    # Load CSV data
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File '{csv_file_path}' not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: File '{csv_file_path}' is empty.")
        return
    except pd.errors.ParseError:
        print(f"Error: Unable to parse file '{csv_file_path}'. Make sure it is a valid CSV file.")
        return

    # Extract relevant data from the DataFrame (adjust column names accordingly)
    member_id_list = df['member_id'].tolist()
    accrual_type_list = df['accrualType'].tolist()
    year_list = df['year'].tolist()
    hours_list = df['hours'].tolist()
    reason_list = df['reason'].tolist()

    # Iterate through data and make API requests
    for i in range(len(member_id_list)):
        accrual_data = {
            "accrualType": accrual_type_list[i],
            "year": year_list[i],
            "hours": hours_list[i],
            "reason": reason_list[i]
        }

        try:
            response = requests.post(url=BASE_URL + f"/system/members/{member_id_list[i]}/accruals", headers=headers, json=accrual_data)
            response.raise_for_status()

            print(f"Uploaded accruals for Member ID {member_id_list[i]}")
            print("Response Code:", response.status_code)
            print("Response Content:", response.text)

        except requests.exceptions.HTTPError as errh:
            print(f"HTTP Error: {errh}")
        except requests.exceptions.RequestExceptions as err:
            print(f"Request Error: {err}")

# Call the function to load and upload accruals
load_and_upload_accruals()
