import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

# API endpoint
endpoint = f"{BASE_URL}/system/members"

# Query parameters for initial request
params = {
    "fields": "identifier,firstName,lastName,vendorNumber,inactiveFlag",  # Request specific fields
    "conditions": "inactiveFlag=false",  # Filter for active members
    "page": 1,  # Start with the first page
    "pageSize": 100  # Set a reasonable page size
}

# List to store all retrieved data
all_members = []

# Loop through all pages
while True:
    # Send GET request to the API
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Add data to the list
        all_members.extend(data)

        # Check if there are more pages
        if len(data) < params["pageSize"]:
            # No more pages to process
            break

        # Move to the next page
        params["page"] += 1
    else:
        # Print error message and exit the loop
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        break

# If data was retrieved, save it to a CSV file
if all_members:
    # Convert to pandas DataFrame
    df = pd.DataFrame(all_members)

    # Specify the output file path
    output_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Members.csv"

    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
else:
    print("No data retrieved.")
