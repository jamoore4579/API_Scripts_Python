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
endpoint = f"{BASE_URL}/sales/orders"

# Parameters for API request
params = {
    "conditions": 'orderDate>=[2025-01-01] AND orderDate<=[2025-01-31]',
    "fields": 'id',
    "page": 1,
    "pageSize": 1000  # Adjust based on API limits
}

# Making the API request
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    sales_order_ids = [order["id"] for order in data]

    # Convert to DataFrame
    df = pd.DataFrame(sales_order_ids, columns=["Sales Order ID"])

    # Define the output file path
    output_path = r"C:\Users\jmoore\Documents\ConnectWise\Integration\NS_Integration\Items\Production\SoProducts.csv"

    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Sales Order IDs successfully saved to {output_path}")
else:
    print(f"Failed to retrieve sales order IDs. Status Code: {response.status_code}, Response: {response.text}")
