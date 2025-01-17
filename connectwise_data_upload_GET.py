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

# File paths
input_file = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\Items\\Production\\Product_Catalog.csv"
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\Items\\Production\\GenericItemsUpdate.csv"

# Read input CSV file
try:
    input_data = pd.read_csv(input_file)
    if 'Name' not in input_data.columns:
        raise KeyError("The 'name' column is missing in the input file.")
except Exception as e:
    print(f"Error reading input file: {e}")
    exit()

# Extract 'name' column values
names = input_data['Name'].dropna().unique()

# API endpoint
endpoint = f"{BASE_URL}/procurement/catalog"

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

# Fetch data for each name
all_results = []
for name in names:
    print(f"Fetching data for name: {name}")
    params = {
        "conditions": f"(identifier like \"{name}\")",
        "fields": "id,identifier,manufacturer/name",
        "pageSize": 1000
    }

    results = fetch_all_pages(endpoint, headers, params)
    if results:
        all_results.extend(results)

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
