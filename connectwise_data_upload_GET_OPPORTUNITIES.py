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

# Setup headers for API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the API endpoint
endpoint = f"{BASE_URL}/sales/opportunities"

# Initialize an empty list to store opportunities with NetSuite ID
opportunities_with_ns_id = []

# Paginate through the results
page = 1
page_size = 100
while True:
    params = {
        'page': page,
        'pageSize': page_size
    }
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}, Response: {response.text}")
        break

    opportunities = response.json()

    # Stop the loop if no more opportunities are found
    if not opportunities:
        break

    # Filter opportunities with a NetSuite ID in customField ID 75
    for opportunity in opportunities:
        custom_fields = opportunity.get("customFields", [])
        for field in custom_fields:
            if field.get("id") == 75 and field.get("value"):
                opportunities_with_ns_id.append({
                    "id": opportunity.get("id"),
                    "netsuite_id": field.get("value")
                })
    
    page += 1

# Create a DataFrame from the filtered opportunities
df = pd.DataFrame(opportunities_with_ns_id)

# Define the output file path
output_file = r"c:\\users\\jmoore\\documents\\connectwise\\Integration\\NS_Integration\\Opportunity\\Production\\OpportunityData.csv"

# Save the DataFrame to a CSV file
df.to_csv(output_file, index=False)

print(f"Data successfully saved to {output_file}")
