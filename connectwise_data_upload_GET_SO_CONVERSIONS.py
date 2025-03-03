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

# API endpoint for sales orders
orders_endpoint = f"{BASE_URL}/sales/orders"

# Parameters for API request to retrieve sales order IDs
params = {
    "conditions": 'orderDate>=[2025-01-01] AND orderDate<=[2025-01-31]',
    "fields": 'id',
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# Making the API request for sales order IDs
response = requests.get(orders_endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    sales_order_ids = [order["id"] for order in data]

    all_products = []

    # Loop through each sales order ID to get its product IDs
    for order_id in sales_order_ids:
        order_details_endpoint = f"{BASE_URL}/sales/orders/{order_id}"
        order_params = {"fields": "productIds"}  # Request only product IDs

        order_response = requests.get(order_details_endpoint, headers=headers, params=order_params)

        if order_response.status_code == 200:
            order_data = order_response.json()
            product_ids = order_data.get("productIds", [])

            for product_id in product_ids:
                # Fetch product details from procurement API
                product_details_endpoint = f"{BASE_URL}/procurement/products/{product_id}"
                product_params = {"fields": "catalogItem/identifier"}

                product_response = requests.get(product_details_endpoint, headers=headers, params=product_params)

                if product_response.status_code == 200:
                    product_data = product_response.json()
                    product_name = product_data.get("catalogItem", {}).get("identifier", "Unknown")
                else:
                    print(f"Failed to retrieve details for Product ID {product_id}. Status Code: {product_response.status_code}")
                    product_name = "Unknown"

                all_products.append({
                    "Sales Order ID": order_id,
                    "Product ID": product_id,
                    "Product Name": product_name
                })
        else:
            print(f"Failed to retrieve product IDs for Sales Order {order_id}. Status Code: {order_response.status_code}")

    # Convert to DataFrame
    df = pd.DataFrame(all_products)

    # Define the output file path
    output_path = r"C:\Users\jmoore\Documents\ConnectWise\Integration\NS_Integration\Items\Production\SoProducts.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Sales order products successfully saved to {output_path}")
else:
    print(f"Failed to retrieve sales order IDs. Status Code: {response.status_code}, Response: {response.text}")
