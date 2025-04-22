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
endpoint = f"{BASE_URL}/company/contacts"

# Fetch all contacts data
def get_contacts():
    all_contacts = []
    page = 1
    page_size = 100  # Adjust as needed

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "fields": "id,firstName,lastName,company/id,company/identifier"
        }

        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_contacts.extend(data)
            page += 1
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    return all_contacts

# Retrieve data
contacts = get_contacts()

# Convert to DataFrame and save as CSV
if contacts:
    df = pd.DataFrame(contacts)
    
    # Output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\Company\\ContactInfo.csv"
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"Data successfully saved to {output_path}")
else:
    print("No data retrieved.")
