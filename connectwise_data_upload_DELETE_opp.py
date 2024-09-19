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

def delete_opportunity(opportunity_id):
    try:
        response = requests.delete(url=BASE_URL + f"/sales/opportunities/{opportunity_id}", headers=headers)
        response.raise_for_status()
        return "Success"
    except requests.exceptions.HTTPError as errh:
        return f"HTTP Error: {errh}"
    except requests.exceptions.RequestException as err:
        return f"Request Error: {err}"

def delete_opportunities_from_csv(file_path, results_path):
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
            status = delete_opportunity(opportunity_id)
            results.append({"opportunity_id": opportunity_id, "status": status})
    
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except pd.errors.EmptyDataError:
        print(f"File '{file_path}' is empty.")
    except pd.errors.ParserError:
        print(f"Error parsing the file '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    # Create a DataFrame from results and save it to a CSV file
    results_df = pd.DataFrame(results)
    results_df.to_csv(results_path, index=False)
    print(f"Results saved to '{results_path}'.")

# Prompt user to upload the CSV file
csv_file_path = input("Please enter the path to the CSV file containing opportunity IDs: ")
results_file_path = input("Please enter the path to save the results CSV file: ")
delete_opportunities_from_csv(csv_file_path, results_file_path)
