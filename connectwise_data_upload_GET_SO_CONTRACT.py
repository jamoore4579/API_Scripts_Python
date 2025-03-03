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

# API endpoint for sales orders
orders_endpoint = f"{BASE_URL}/sales/orders"

# Parameters for API request (Sales Orders)
params = {
    "conditions": "department/id=23 AND orderDate>=[2025-01-01] AND orderDate<=[2025-01-31]",
    "fields": "id,opportunity/id",
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# Initialize list to store sales order data
sales_orders = []

# Fetch all sales orders, handling pagination
while True:
    response = requests.get(orders_endpoint, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Append order details to the list
        for order in data:
            sales_orders.append({
                "id": order.get("id"),
                "opportunity_id": order.get("opportunity", {}).get("id")
            })
        
        # Check if there are more pages
        if len(data) < params["pageSize"]:
            break  # No more pages to fetch
        else:
            params["page"] += 1  # Increment page number
    else:
        print(f"Error {response.status_code}: {response.text}")
        break

# Convert sales orders to DataFrame
df_sales_orders = pd.DataFrame(sales_orders)

# Initialize list for enriched sales order data with additional details
enriched_sales_orders = []

# API endpoints
opportunity_endpoint = f"{BASE_URL}/sales/opportunities"
forecast_endpoint = f"{BASE_URL}/sales/opportunities"

# Function to retrieve custom fields, company identifier, and forecast revenue from an opportunity
def get_opportunity_details(opportunity_id):
    if not opportunity_id:
        return None, None, None, None, None  # Return None for all if no opportunity ID

    url = f"{opportunity_endpoint}/{opportunity_id}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        opportunity_data = response.json()
        custom_fields = opportunity_data.get("customFields", [])
        company_identifier = opportunity_data.get("company", {}).get("identifier")

        # Extract custom field values and ensure explicit None if missing
        field_63 = next((cf.get("value") for cf in custom_fields if cf.get("id") == 63), None)
        field_64 = next((cf.get("value") for cf in custom_fields if cf.get("id") == 64), None)
        field_65 = next((cf.get("value") for cf in custom_fields if cf.get("id") == 65), None)

        # Ensure missing values are explicitly set to None
        field_63 = field_63 if field_63 else None
        field_64 = field_64 if field_64 else None
        field_65 = field_65 if field_65 else None

        # Retrieve forecast revenue (Order Total)
        order_total = get_forecast_revenue(opportunity_id)

        return field_63, field_64, field_65, company_identifier, order_total
    else:
        print(f"Error fetching opportunity {opportunity_id}: {response.status_code}")
        return None, None, None, None, None  # Return None if error occurs

# Function to retrieve forecast revenue (Order Total)
def get_forecast_revenue(opportunity_id):
    url = f"{forecast_endpoint}/{opportunity_id}/forecast"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        forecast_data = response.json()
        return forecast_data.get("forecastRevenueTotals", {}).get("revenue") or None  # Ensure None if missing
    else:
        print(f"Error fetching forecast for opportunity {opportunity_id}: {response.status_code}")
        return None  # Return None if error occurs

# Iterate over sales orders to enrich with opportunity custom fields, company identifier, and order total
for order in sales_orders:
    opp_id = order["opportunity_id"]
    field_63, field_64, field_65, company_identifier, order_total = get_opportunity_details(opp_id)

    enriched_sales_orders.append({
        "order_id": order["id"],
        "opportunity_id": opp_id,
        "company_identifier": company_identifier or None,  # Ensure None if missing
        "AL-Purchasing Contract": field_63,
        "MS-Purchasing Contract": field_64,
        "NAT-Purchasing Contract": field_65,
        "Order Total": order_total
    })

# Convert enriched data to DataFrame
df_enriched = pd.DataFrame(enriched_sales_orders)

# Explicitly set empty contract values to None
for column in ["AL-Purchasing Contract", "MS-Purchasing Contract", "NAT-Purchasing Contract"]:
    df_enriched[column] = df_enriched[column].apply(lambda x: None if pd.isna(x) or x == "" else x)

# Define output file path
output_path = r"C:\Users\jmoore\Documents\ConnectWise\Integration\NS_Integration\SalesOrders\SoContracts.csv"

# Save to CSV
df_enriched.to_csv(output_path, index=False, na_rep="None")  # Ensures None appears in the file

print(f"Sales orders with opportunity custom fields, company identifier, and Order Total saved to {output_path}")
