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

# Define API endpoint for products
products_endpoint = f"{BASE_URL}/procurement/products"

# Define product identifier prefixes to filter
valid_prefixes = ("AP9-A", "AP9-B", "AP7-B", "AP7-U", "AP7E-B", "AP7E-U")

# Define query parameters
params = {
    "conditions": "(warehouseId=3) OR (warehouseId=4)",
    "fields": "catalogItem/identifier,salesOrder/id,invoice/identifier",
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# List to store filtered products
products = []

# Fetch all product data with pagination
while True:
    response = requests.get(products_endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        break

    data = response.json()
    
    # Extract and filter product identifiers
    if data:
        for item in data:
            identifier = item.get("catalogItem", {}).get("identifier", "N/A")
            sales_order_id = item.get("salesOrder", {}).get("id", "N/A")
            invoice_identifier = item.get("invoice", {}).get("identifier", "N/A")

            # Filter only products with the specified prefixes
            if identifier.startswith(valid_prefixes):  # Filter by prefixes
                products.append({
                    "Product Identifier": identifier,
                    "Sales Order ID": sales_order_id,
                    "Invoice Identifier": invoice_identifier
                })

    # Pagination handling
    if len(data) < params["pageSize"]:
        break  # Exit loop if last page is reached

    params["page"] += 1  # Increment page number

# Output file path
output_file = r"C:\users\jmoore\documents\connectwise\products\Product032525.csv"

# Save filtered data to CSV
if products:
    df = pd.DataFrame(products)
    df.to_csv(output_file, index=False)
    print(f"Filtered products exported successfully to {output_file}")
else:
    print("No matching products retrieved.")
