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
API_ENDPOINT = f"{BASE_URL}/sales/opportunities"

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Pagination variables
page = 1
page_size = 1000
all_opportunities = []

while True:
    # Query parameters for pagination and ID filtering
    params = {
        "page": page,
        "pageSize": page_size,
        "fields": "id,name,stage/name,status/name,customFields",
        "conditions": "id>=16000"  # Only retrieve opportunities with ID >= 16000
    }

    # API request
    response = requests.get(API_ENDPOINT, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        print(f"API Response Type: {type(data)}")  # Debugging: check response structure

        # Ensure we extract opportunities correctly
        if isinstance(data, list):
            opportunities = data  # The API returned a list directly
        elif isinstance(data, dict):  
            opportunities = data.get("opportunities", [])  # Adjust key if needed based on API response
        else:
            print("Unexpected data format:", data)
            break

        if not opportunities:
            print("No more opportunities found. Stopping pagination.")
            break

        # Extract only custom field ID 27
        for opp in opportunities:
            custom_fields = opp.get("customFields", [])
            
            # Find custom field ID 27 safely
            custom_field_27 = None
            for field in custom_fields:
                if field.get("id") == 27:
                    custom_field_27 = field.get("value", None)  # Use .get() to prevent KeyError

            filtered_opp = {
                "id": opp.get("id"),
                "name": opp.get("name"),
                "stage_name": opp.get("stage", {}).get("name"),
                "status_name": opp.get("status", {}).get("name"),
                "sf_opportunity_id": custom_field_27
            }
            
            all_opportunities.append(filtered_opp)

        page += 1  # Move to the next page

    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        break

# Convert to DataFrame
df = pd.DataFrame(all_opportunities)

if not df.empty:
    # Define output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\Opportunity\\opportunity_stage.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Filtered sales opportunities successfully saved to: {output_path}")
else:
    print("No data to save. No opportunities met the filter criteria.")
