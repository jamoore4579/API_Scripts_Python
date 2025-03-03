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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# API endpoint for procurement adjustments
endpoint = f"{BASE_URL}/procurement/adjustments"

# Parameters for API request
params = {
    "conditions": 'closedBy like "TimK%" AND closedDate > [2025-01-08]',
    "fields": 'id',
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# List to store all retrieved adjustment IDs
adjustment_ids = []

# Pagination loop to fetch all pages
while True:
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json()

        if not data or len(data) == 0:
            break  # Stop when there's no more data

        # Extract IDs
        adjustment_ids.extend(item['id'] for item in data)
        params["page"] += 1  # Move to the next page

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        break
    except ValueError:
        print("Invalid JSON response received")
        break

# List to store detailed data
detailed_data = []

# Fetch details for each adjustment ID
for adjustment_id in adjustment_ids:
    detail_endpoint = f"{BASE_URL}/procurement/adjustments/{adjustment_id}/details"

    try:
        detail_response = requests.get(detail_endpoint, headers=headers)
        if detail_response.status_code != 200:
            print(f"Error fetching details for ID {adjustment_id}: {detail_response.status_code} - {detail_response.text}")
            continue

        detail_data = detail_response.json()
        if detail_data:
            for item in detail_data:
                # Extract all fields and modify specific ones
                refined_data = item.copy()  # Copy full response

                # Extract specific values for adjustment and _info
                refined_data["adjustment"] = item.get("adjustment", {}).get("name", "")  # Only include name
                refined_data["updatedBy"] = item.get("_info", {}).get("updatedBy", "")  # Only include updatedBy
                refined_data["catalogItem"] = item.get("catalogItem", {}).get("identifier", "")  # Only include identifier

                # Remove unwanted columns
                for column in ["serialNumber", "warehouseBin", "warehouse", "description", "quantityOnHand", "id", "_info"]:
                    refined_data.pop(column, None)

                detailed_data.append(refined_data)

    except requests.exceptions.RequestException as e:
        print(f"Request failed for ID {adjustment_id}: {e}")

# Convert refined data to DataFrame
df_details = pd.DataFrame(detailed_data)

# Output file path
output_path = r"C:\users\jmoore\documents\connectwise\integration\ns_integration\Items\Production\InvAdjustmentDetails.csv"

# Save to CSV
df_details.to_csv(output_path, index=False)

print(f"Data successfully saved to {output_path}")
