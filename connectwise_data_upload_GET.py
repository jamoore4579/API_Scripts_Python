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
endpoint = f"{BASE_URL}/sales/opportunities"

# Parameters for API request (Filtering for 'New' opportunity category)
params = {
    "conditions": "status/id=3 OR status/id=5 AND id>=16000",
    "fields": "id,name,status,name,customFields",
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# List to store all retrieved data
all_opportunities = []

# Pagination loop to fetch all pages
while True:
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break  # Exit the loop on error

    data = response.json()  # API response

    # Ensure data is a list (adjust based on API response format)
    if isinstance(data, list):
        opportunities = data  # API returned a list directly
    elif isinstance(data, dict):
        opportunities = data.get("items", [])  # If API response is a dictionary
    else:
        print("Unexpected API response format")
        break

    if not opportunities:
        break  # Stop when there's no more data

    # Extract necessary fields and filter by Opportunity Category = "New"
    for item in opportunities:
        custom_fields = item.get("customFields", [])

        # Extract specific custom field value (e.g., id 45)
        field_45 = next((cf.get("value") for cf in custom_fields if cf.get("id") == 45), None)

        # Only include opportunities where Opportunity Category = "New"
        if field_45 and field_45.lower() == "new":
            all_opportunities.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "status": item.get("status", {}).get("name"),  # Extract only the status name
                "Opportunity Category": field_45,  # Include extracted custom field
            })

    params["page"] += 1  # Move to the next page

# Convert to DataFrame
df = pd.DataFrame(all_opportunities)

# Output file path
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\ns_integration\\statusAudit.csv"

# Save to CSV
df.to_csv(output_path, index=False)

print(f"Data successfully saved to {output_path}")
