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
    "Authorization": "Basic " + AUTH_CODE
}

# File path for output
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\Items\\Production\\GenericItemsUpdate.csv"

# API endpoint
endpoint = f"{BASE_URL}/procurement/catalog"

# Function to fetch all pages of data
def fetch_all_products(endpoint, headers, params):
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

# Parameters for fetching all products
params = {
    "fields": "id,identifier,cost",
    "pageSize": 1000
}

# Fetch all products
print("Fetching all products from the catalog...")
all_results = fetch_all_products(endpoint, headers, params)

# Check if any data was retrieved
if all_results:
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(all_results)

    # Save DataFrame to CSV
    try:
        df.to_csv(output_path, index=False)
        print(f"Data successfully written to {output_path}")
    except Exception as e:
        print(f"Error writing output file: {e}")
else:
    print("No data retrieved.")
