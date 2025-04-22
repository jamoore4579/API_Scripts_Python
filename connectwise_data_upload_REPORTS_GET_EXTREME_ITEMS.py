import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Validate environment variables
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing one or more environment variables: BASE_URL, AUTH_CODE, CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Define API endpoint
catalog_endpoint = f"{BASE_URL}/procurement/catalog"

# Define base parameters
params = {
    "conditions": '(manufacturer/name like "extreme networks%" AND serializedFlag = true)',
    "fields": "id,identifier,manufacturer/name,serializedFlag",
    "page": 1,
    "pageSize": 1000
}

# List to store catalog items
items = []

# Fetch all pages
while True:
    response = requests.get(catalog_endpoint, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    if not data:
        break

    items.extend(data)

    if len(data) < params["pageSize"]:
        break

    params["page"] += 1

# Convert to DataFrame and save
df = pd.DataFrame(items)
output_file = r"C:\users\jmoore\documents\connectwise\products\ExtremeProducts.csv"
df.to_csv(output_file, index=False)
print(f"Saved {len(df)} items to {output_file}")
