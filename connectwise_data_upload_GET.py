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

def get_all_open_opportunities():
    try:
        status_ids = [1, 2, 16, 17]
        condition_query = " OR ".join([f"status/id={status_id}" for status_id in status_ids])
        params = {
            "conditions": condition_query,
            "pageSize": 1000
        }
        response = requests.get(url=BASE_URL + "/sales/opportunities", headers=headers, params=params)
        response.raise_for_status()
        opportunities = response.json()
        return opportunities
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return []
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return []

def check_opportunity_status(opportunity_id):
    try:
        response = requests.get(url=BASE_URL + f"/sales/opportunities/{opportunity_id}", headers=headers)
        response.raise_for_status()
        opportunity_data = response.json()
        return opportunity_data
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return None

def extract_opportunity_data(opportunity_data):
    custom_fields = opportunity_data.get("customFields", [])
    sf_opportunity_link = next((item for item in custom_fields if item["id"] == 21), {}).get("value")
    sf_opportunity_id = next((item for item in custom_fields if item["id"] == 27), {}).get("value")
    se_name = next((item for item in custom_fields if item["id"] == 8), {}).get("value")
    bde_name = next((item for item in custom_fields if item["id"] == 51), {}).get("value")

    return {
        "id": opportunity_data.get("id"),
        "name": opportunity_data.get("name"),
        "expectedCloseDate": opportunity_data.get("expectedCloseDate"),
        "closedDate": opportunity_data.get("closedDate"),
        "stage": opportunity_data.get("stage", {}).get("id"),
        "status": opportunity_data.get("status", {}).get("id"),
        "primarySalesRep": opportunity_data.get("primarySalesRep", {}).get("id"),
        "locationId": opportunity_data.get("locationId"),
        "businessUnitId": opportunity_data.get("businessUnitId"),
        "company": opportunity_data.get("company", {}).get("id"),
        "contact": opportunity_data.get("contact", {}).get("id"),
        "SF_Opportunity_Link": sf_opportunity_link,
        "SF_Opportunity_ID": sf_opportunity_id,
        "SE_Name": se_name,
        "BDE_Name": bde_name
    }

def upload_cw_data():
    # Fetch all relevant opportunities
    opportunities = get_all_open_opportunities()

    if not opportunities:
        print("No relevant opportunities found.")
        return

    # Create a list to store results
    results = []

    # Iterate through each opportunity
    for opportunity in opportunities:
        opportunity_id = opportunity.get('id')
        opportunity_data = check_opportunity_status(opportunity_id)
        
        if opportunity_data:
            data_to_store = extract_opportunity_data(opportunity_data)
            results.append(data_to_store)

    # Write results to CSV file
    results_df = pd.DataFrame(results)
    results_file_path = r'c:\users\jmoore\documents\connectwise\opportunity_data.csv'
    results_df.to_csv(results_file_path, index=False)
    print(f"Opportunity data written to '{results_file_path}'.")

# Call the function to load and upload data
upload_cw_data()
