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

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# Define API endpoint for sales orders
orders_endpoint = f"{BASE_URL}/sales/orders"

# Define query parameters
params = {
    "conditions": "orderDate>=[2025-02-01] AND orderDate<=[2025-02-28]",
    "fields": "id,orderDate,status/id,status/name,productIds",
    "page": 1,
    "pageSize": 1000  # Adjust as needed
}

# List to store all sales orders
sales_orders = []

# Fetch all pages of data
while True:
    response = requests.get(orders_endpoint, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if not data:
            break  # Exit loop if no more data
        
        sales_orders.extend(data)
        
        # Retrieve total record count for proper pagination (if available)
        total_records = int(response.headers.get("X-Total-Count", 0))
        
        # Calculate the number of pages needed
        total_pages = (total_records // params["pageSize"]) + (1 if total_records % params["pageSize"] > 0 else 0)
        
        if params["page"] >= total_pages:
            break  # Exit if we've reached the last page
        
        params["page"] += 1  # Increment page number for pagination
    else:
        print(f"Error {response.status_code}: {response.text}")
        break

# Function to lookup product details using direct product ID with fields parameter
def lookup_product_details(product_id):
    """Retrieve catalogItem details for a given product ID using direct API endpoint with fields parameter."""
    product_endpoint = f"{BASE_URL}/procurement/products/{product_id}"
    product_params = {"fields": "catalogItem/id,catalogItem/identifier"}
    
    response = requests.get(product_endpoint, headers=headers, params=product_params)
    
    if response.status_code == 200:
        product_data = response.json()
        if product_data:
            return product_data.get("catalogItem", {}).get("id", None), product_data.get("catalogItem", {}).get("identifier", None)
    else:
        print(f"Error retrieving product {product_id}: {response.status_code} - {response.text}")
    
    return None, None  # Return None if product details are not found

# Enhance sales orders data with product details
enhanced_orders = []

for order in sales_orders:
    product_details = []
    
    if order.get("productIds"):  # Check if the order has product IDs
        for product_id in order["productIds"]:
            catalog_id, identifier = lookup_product_details(product_id)
            product_details.append({
                "productId": product_id,
                "catalogItemId": catalog_id,
                "catalogIdentifier": identifier
            })
    
    # Append enhanced order details
    enhanced_orders.append({
        "id": order["id"],
        "orderDate": order["orderDate"],
        "statusId": order["status"]["id"],
        "statusName": order["status"]["name"],
        "products": product_details
    })

# Convert to DataFrame and save to CSV
if enhanced_orders:
    df = pd.json_normalize(enhanced_orders, "products", ["id", "orderDate", "statusId", "statusName"])
    output_file = r"C:\users\jmoore\documents\connectwise\sales orders\SalesOrders_Feb2025.csv"
    df.to_csv(output_file, index=False)
    print(f"Data successfully saved to {output_file}")
else:
    print("No sales orders found for the specified conditions.")
