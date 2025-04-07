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

# Input file path
input_path = r"C:\\users\\jmoore\\documents\\connectwise\\Company\\SiteData.csv"

# Output file path
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\Company\\CompanySiteInfo.csv"

# Read the input file to get CWIDs
try:
    df_input = pd.read_csv(input_path, dtype={"CWID": str})  # Ensure 'CWID' is read as a string
    if "CWID" not in df_input.columns:
        raise ValueError("The input CSV must contain an 'CWID' column.")
except Exception as e:
    print(f"Error reading input file: {e}")
    exit(1)

# Function to fetch site data for a given CWID
def get_company_sites(cw_id):
    endpoint = f"{BASE_URL}/company/companies/{cw_id}/sites"
    params = {
        "fields": "id,company/name,name,addressLine1,city,stateReference/identifier,zip,phoneNumber"
    }
    response = requests.get(endpoint, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for CWID {cw_id}: {response.status_code} - {response.text}")
        return None

# Process each CWID from the input file
all_sites = []

for cw_id in df_input["CWID"]:
    site_data = get_company_sites(cw_id)
    
    if site_data:
        for site in site_data:
            all_sites.append({
                "Company ID": cw_id,
                "Company Name":site.get("company", {}).get("name", ""),
                "Site ID": site.get("id", ""),
                "Site Name": site.get("name", ""),
                "Address": site.get("addressLine1", ""),
                "City": site.get("city", ""),
                "State": site.get("stateReference", {}).get("identifier", ""),
                "Zip": site.get("zip", ""),
                "Phone Number": site.get("phoneNumber", "")
            })

# Convert to DataFrame and save as CSV
if all_sites:
    df_sites = pd.DataFrame(all_sites)
    df_sites.to_csv(output_path, index=False)
    print(f"Data successfully saved to {output_path}")
else:
    print("No data retrieved.")
