import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_SAND = os.getenv("BASE_SAND")
AUTH_SAND = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_SAND,
    "Content-Type": "application/json"  # Ensure Content-Type is set to JSON
}

def add_items_to_connectwise(product_data):
    try:
        response = requests.post(url=BASE_SAND + "/procurement/catalog", headers=headers, json=product_data)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX, 5XX)
        print(f"Product '{product_data['identifier']}' successfully added.")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error for product {product_data['identifier']}: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"Request Error for product {product_data['identifier']}: {err}")

def upload_cw_data(csv_file_path):
    # Read the CSV file containing products to be added
    try:
        product_df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: The file at path {csv_file_path} does not exist.")
        return
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
        return
    except pd.errors.ParserError:
        print("Error: There was a problem parsing the CSV file.")
        return

    if product_df.empty:
        print("No products found in the CSV file.")
        return

    # Iterate through each product in the CSV
    for index, product in product_df.iterrows():
        product_data = {
            "identifier": product.get("Name"),  # Identifier from 'Name' column
            "description": product.get("Name"),  # Description from 'Name' column
            "inactiveFlag": False,  # Hardcoded as per example
            "subcategory": {
                "id": 140  # Hardcoded subcategory ID as per example
            },
            "type": {
                "id": 34  # Hardcoded type ID as per example
            },
            "customerDescription": product.get("Description")  # Customer description from 'Description' column
        }

        # Add product to ConnectWise using the Procurement API
        add_items_to_connectwise(product_data)

    print("All products have been processed and uploaded.")

# Ask the user to input the file path
csv_file_path = input("Please enter the path of the CSV file to upload: ")

# Call the function to load and upload data
upload_cw_data(csv_file_path)
