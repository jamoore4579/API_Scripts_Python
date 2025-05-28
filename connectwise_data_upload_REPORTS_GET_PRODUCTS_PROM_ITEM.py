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

# Define base query parameters
params = {
    # "conditions": "(warehouseId=3) OR (warehouseId=4)",
    "fields": "catalogItem/identifier,id",
    "page": 1,
    "pageSize": 1000
}

# List to store filtered products
products = []

while True:
    response = requests.get(products_endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code} - {response.text}")
        break

    data = response.json()

    if not data:
        break

    for product in data:
        identifier = product.get("catalogItem", {}).get("identifier", "")
        if identifier.startswith(valid_prefixes):
            products.append(product)

    if len(data) < params["pageSize"]:
        break

    params["page"] += 1

# New function to fetch picking and shipping details
def fetch_shipping_details(product_id):
    url = f"{BASE_URL}/procurement/products/{product_id}/pickingShippingDetails"
    details_params = {"fields": "shippedQuantity,shipmentDate"}
    response = requests.get(url, headers=headers, params=details_params)

    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list) and data:
            # Return the first shipping record
            return data[0]
        else:
            return {}
    else:
        print(f"Failed to get shipping details for product ID {product_id}: {response.status_code}")
        return {}


# Add shipping details to each product
for product in products:
    product_id = product.get("id")
    details = fetch_shipping_details(product_id)
    product.update(details)

# Output file path
output_file = r"C:\\users\\jmoore\\documents\\connectwise\\products\\Product050625.csv"

# Save filtered data to CSV
if products:
    df = pd.DataFrame(products)
    df.to_csv(output_file, index=False)
    print(f"Filtered products exported successfully to {output_file}")
else:
    print("No matching products retrieved.")
