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
API_ENDPOINT = f"{BASE_URL}/sales/orders"

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Pagination variables
page = 1
page_size = 1000
all_orders = []

while True:
    # Query parameters for pagination, updated to filter orders where customField id 83 has a value
    params = {
        "customFieldConditions": "caption='Netsuite ID' AND value != null",
        "page": page,
        "pageSize": page_size,
        "fields": "id,company/name,department/name,customFields"
    }

    # API request
    response = requests.get(API_ENDPOINT, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        print(f"API Response Type: {type(data)}")  # Debugging: check response structure

        # Ensure we extract orders correctly
        if isinstance(data, list):
            orders = data  # The API returned a list directly
        elif isinstance(data, dict):  
            orders = data.get("orders", [])  # Adjust this key if needed based on API response
        else:
            print("Unexpected data format:", data)
            break

        if not orders:
            break  # Stop if no more orders

        for order in orders:
            # Extracting only the value of customField with id 83
            netsuite_id = None
            custom_fields = order.get("customFields", [])
            for field in custom_fields:
                if field.get("id") == 83:
                    netsuite_id = field.get("value")
                    break  # Stop searching after finding the required field

            # Construct cleaned-up order data
            all_orders.append({
                "id": order.get("id"),
                "company_name": order.get("company", {}).get("name"),
                "department_name": order.get("department", {}).get("name"),
                "netsuite_id": netsuite_id
            })

        page += 1  # Move to the next page
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        break

# Convert to DataFrame
df = pd.DataFrame(all_orders)

if not df.empty:
    # Define output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\sales orders\\netsuite_salesorders.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"All sales orders successfully saved to: {output_path}")
else:
    print("No sales orders found with a value for customField id 83.")