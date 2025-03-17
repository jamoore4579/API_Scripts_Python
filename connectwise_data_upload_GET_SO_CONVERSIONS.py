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
    "conditions": 'orderDate>=[2025-01-01] AND orderDate<=[2025-03-04] AND (department/id=23 OR department/id=26)',
    "fields": 'id',
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

def get_conversions(order_id):
    """
    Retrieve conversions for a given sales order ID.
    """
    conversions_endpoint = f"{BASE_URL}/sales/orders/conversions/{order_id}"
    conversions_response = requests.get(conversions_endpoint, headers=headers)

    if conversions_response.status_code == 200:
        conversions_data = conversions_response.json()
        return conversions_data  # Return conversion data as-is (list or dict)
    else:
        print(f"Failed to retrieve conversions for Sales Order {order_id}. Status Code: {conversions_response.status_code}")
        return None

def get_custom_field_value(product_name):
    """
    Lookup product in the catalog using identifier and retrieve customField ID 80.
    """
    catalog_endpoint = f"{BASE_URL}/procurement/catalog"
    catalog_params = {"conditions": f'identifier like "{product_name}%"', "fields": "customFields"}
    
    catalog_response = requests.get(catalog_endpoint, headers=headers, params=catalog_params)

    if catalog_response.status_code == 200:
        catalog_data = catalog_response.json()
        if catalog_data:
            # Extract custom field ID 80
            for field in catalog_data[0].get("customFields", []):
                if field.get("id") == 80:
                    return field.get("value", "N/A")  # Return custom field value or "N/A" if not found
    else:
        print(f"Failed to retrieve catalog data for {product_name}. Status Code: {catalog_response.status_code}")
    
    return "N/A"  # Default value if not found

# Making the API request for sales order IDs
response = requests.get(orders_endpoint, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    sales_order_ids = [order["id"] for order in data]

    all_products = []

    # Loop through each sales order ID to get its product IDs and conversions
    for order_id in sales_order_ids:
        order_details_endpoint = f"{BASE_URL}/sales/orders/{order_id}"
        order_params = {"fields": "productIds"}  # Request only product IDs

        order_response = requests.get(order_details_endpoint, headers=headers, params=order_params)

        if order_response.status_code == 200:
            order_data = order_response.json()
            product_ids = order_data.get("productIds", [])

            # Fetch conversion data
            conversions = get_conversions(order_id)

            # Convert conversions data to a string (or handle differently based on format)
            conversions_str = ", ".join([str(conv) for conv in conversions]) if conversions else "None"

            for product_id in product_ids:
                # Fetch product details from procurement API
                product_details_endpoint = f"{BASE_URL}/procurement/products/{product_id}"
                product_params = {"fields": "catalogItem/identifier"}

                product_response = requests.get(product_details_endpoint, headers=headers, params=product_params)

                if product_response.status_code == 200:
                    product_data = product_response.json()
                    product_name = product_data.get("catalogItem", {}).get("identifier", "Unknown")

                    # Fetch custom field value for the product
                    custom_field_value = get_custom_field_value(product_name)
                else:
                    print(f"Failed to retrieve details for Product ID {product_id}. Status Code: {product_response.status_code}")
                    product_name = "Unknown"
                    custom_field_value = "N/A"

                all_products.append({
                    "Sales Order ID": order_id,
                    "Product ID": product_id,
                    "Product Name": product_name,
                    "Renewable": custom_field_value,  # New column for custom field value
                    "Conversions": conversions_str  # Adding conversion data
                })
        else:
            print(f"Failed to retrieve product IDs for Sales Order {order_id}. Status Code: {order_response.status_code}")

    # Convert to DataFrame
    df = pd.DataFrame(all_products)

    # Define the output file path
    output_path = r"C:\Users\jmoore\Documents\ConnectWise\Sales Orders\SoProducts.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Sales order products successfully saved to {output_path}")
else:
    print(f"Failed to retrieve sales order IDs. Status Code: {response.status_code}, Response: {response.text}")
