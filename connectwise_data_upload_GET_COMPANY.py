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

# API endpoint
endpoint = f"{BASE_URL}/company/companies"

# Fetch all companies data
def get_companies():
    all_companies = []
    page = 1
    page_size = 100  # Adjust as needed

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "conditions": "deletedFlag=false AND isVendorFlag=false AND (status/id=1 OR status/id=13 OR status/id=17 OR status/id=21)"  # Ensure only active companies are retrieved
        }

        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_companies.extend(data)
            page += 1
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    return all_companies

# Retrieve data
companies = get_companies()

# Convert to DataFrame and save as CSV
if companies:
    df = pd.DataFrame(companies)
    
    # Output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\Company\\Production\\CompanySite.csv"
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"Data successfully saved to {output_path}")
else:
    print("No data retrieved.")
