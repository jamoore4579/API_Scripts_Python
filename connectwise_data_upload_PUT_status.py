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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

def update_opportunity_status(opportunity_id, data_to_store):
    try:
        url = BASE_URL + f"/sales/opportunities/{opportunity_id}"
        data = {
            "id": data_to_store["id"],
            "name": data_to_store["name"],
            "expectedCloseDate": data_to_store["expectedCloseDate"],
            "stage": {"id": data_to_store["stage"]},
            "status": {"id": data_to_store["status"]},
            "primarySalesRep": {"id": data_to_store["primarySalesRep"]},
            "locationId": data_to_store["locationId"],
            "businessUnitId": data_to_store["businessUnitId"],
            "company": {"id": data_to_store["company"]},
            "contact": {"id": data_to_store["contact"]}
        }
        response = requests.put(url=url, json=data, headers=headers)
        response.raise_for_status()
        opportunity_data = response.json()
        return opportunity_data
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        print("Response Content:", response.content)
        return None
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return None

def upload_cw_data():
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
    except pd.errors.ParserError:
        print(f"Error: Unable to parse file '{csv_file_path}'. Make sure it is a valid CSV file.")
        return

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        opportunity_id = row['id']
        
        # Construct data to store
        data_to_store = {
            "id": row["id"],
            "name": row["name"],
            "expectedCloseDate": row["expectedCloseDate"],
            "stage": row["stage"],
            "status": row["status"],
            "primarySalesRep": row["primarySalesRep"],
            "locationId": row["locationId"],
            "businessUnitId": row["businessUnitId"],
            "company": row["company"],
            "contact": row["contact"]
        }
        
        # Update opportunity status
        opportunity_data = update_opportunity_status(opportunity_id, data_to_store)
        
        if opportunity_data:
            print(f"Opportunity {opportunity_id} updated successfully.")

# Call the function to load and upload data
upload_cw_data()
