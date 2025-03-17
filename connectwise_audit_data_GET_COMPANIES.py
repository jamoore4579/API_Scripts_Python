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

# API endpoint
API_ENDPOINT = f"{BASE_URL}/company/companies"

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Pagination variables
page = 1
page_size = 1000
all_items = []

while True:
    # Query parameters for pagination
    params = {
        "conditions": "deletedFlag=false AND (status/id=1 OR status/id=13 OR status/id=21)",
        "fields": "id,name,state,billingTerms/name",
        "page": page,
        "pageSize": page_size
    }

    # API request
    response = requests.get(API_ENDPOINT, headers=headers, params=params)

    # Check if request was successful
    if response.status_code == 200:
        data = response.json()

        if not data:
            print(f"All pages retrieved. Total items collected: {len(all_items)}")
            break  # Exit loop if no more data

        # Append the data to the list
        all_items.extend(data)
        print(f"Page {page} retrieved with {len(data)} records.")

        # Move to the next page
        page += 1
    else:
        print(f"Failed to retrieve data: {response.status_code} - {response.text}")
        break

# Convert collected data to DataFrame and save as CSV
if all_items:
    df = pd.DataFrame(all_items)

    # Define output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\audit\\companies_state.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"All catalog items successfully saved to: {output_path}")
else:
    print("No active catalog items found.")