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

def get_opportunity_details(opportunity_id):
    try:
        response = requests.get(url=BASE_URL + f"/sales/opportunities/{opportunity_id}", headers=headers)
        response.raise_for_status()
        data = response.json()
        return {
            "opportunity_id": opportunity_id,
            "stage": data.get("stage"),
            "status": data.get("status")
        }
    except requests.exceptions.HTTPError as errh:
        return {"opportunity_id": opportunity_id, "stage": None, "status": f"HTTP Error: {errh}"}
    except requests.exceptions.RequestException as err:
        return {"opportunity_id": opportunity_id, "stage": None, "status": f"Request Error: {err}"}

def get_opportunities_from_csv(file_path):
    results = []
    try:
        # Load CSV file
        df = pd.read_csv(file_path)
        
        # Check if the required 'opportunity_id' column exists
        if 'opportunity_id' not in df.columns:
            print("CSV file must contain an 'opportunity_id' column.")
            return
        
        # Iterate through each opportunity ID in the file
        for index, row in df.iterrows():
            opportunity_id = row['opportunity_id']
            details = get_opportunity_details(opportunity_id)
            results.append(details)
    
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except pd.errors.EmptyDataError:
        print(f"File '{file_path}' is empty.")
    except pd.errors.ParserError:
        print(f"Error parsing the file '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    # Write results to CSV file
    results_df = pd.DataFrame(results)
    results_file_path = r'c:\users\jmoore\documents\connectwise\data_output.csv'
    results_df.to_csv(results_file_path, index=False)
    print(f"Output data written to '{results_file_path}'.")

# Prompt user to upload the CSV file
csv_file_path = input("Please enter the path to the CSV file containing opportunity IDs: ")
get_opportunities_from_csv(csv_file_path)
