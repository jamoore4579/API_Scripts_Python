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

# Validate environment variables
if not BASE_URL or not AUTH_CODE or not CLIENT_ID:
    print("Missing required environment variables. Please check your .env file.")
    exit()

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}"
}

# File path for output
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\Opportunity\\Production\\OpportunitiesUpdate.csv"

# API endpoints
opportunities_endpoint = f"{BASE_URL}/sales/opportunities"
departments_endpoint = f"{BASE_URL}/system/departments"

# Function to fetch all pages of data
def fetch_all_pages(endpoint, headers, params):
    all_data = []
    page = 1

    while True:
        params["page"] = page
        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)
            break

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        page += 1

    return all_data

# Fetch all active opportunities
print("Fetching all active opportunities...")
params = {
    "conditions": "(status/id=1)",
    "fields": "id,locationId,businessUnitId,stage/name,status/name,primarySalesRep/name",
    "pageSize": 1000
}

opportunities = fetch_all_pages(opportunities_endpoint, headers, params)

# Fetch all departments
print("Fetching departments...")
departments = fetch_all_pages(departments_endpoint, headers, params={})

# Create a mapping of businessUnitId to department name
department_lookup = {str(dept['id']): dept['name'] for dept in departments}

# Replace businessUnitId with department name in opportunities
def map_department_name(opportunity):
    business_unit_id = str(opportunity.get("businessUnitId", ""))
    opportunity["business_unit_id"] = department_lookup.get(business_unit_id, "Unknown")
    return {
        "id": opportunity.get("id"),
        "locationId": opportunity.get("locationId"),
        "business_unit_id": opportunity.get("business_unit_id"),
        "stage/name": opportunity.get("stage", {}).get("name", "Unknown"),
        "status/name": opportunity.get("status", {}).get("name", "Unknown"),
        "primarySalesRep/name": opportunity.get("primarySalesRep", {}).get("name", "Unknown")
    }

if opportunities:
    print("Mapping department names to opportunities...")
    mapped_opportunities = [map_department_name(op) for op in opportunities]

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(mapped_opportunities)

    # Save DataFrame to CSV
    try:
        df.to_csv(output_path, index=False)
        print(f"Data successfully written to {output_path}")
    except Exception as e:
        print(f"Error writing output file: {e}")
else:
    print("No opportunities data retrieved.")
