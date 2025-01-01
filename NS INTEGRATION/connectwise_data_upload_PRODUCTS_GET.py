import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Check if essential environment variables are loaded
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing required environment variables. Please check your .env file.")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}"
}

# Define the endpoint and parameters for the request
endpoint = f"{BASE_URL}/procurement/catalog"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Items\\CW_ALL_VAR_PRODUCTS_121824.csv"

# Initial parameters for pagination
params = {
    "conditions": "inactiveFlag = false AND notes like 'Import 120324%'",  # Filter by active products
    "fields": "id,identifier,category/name,subcategory/name,type/name", 
    "pagesize": 1000,  # Number of records per page
    "page": 1  # Start from the first page
}

all_data = []  # To collect all pages of data

try:
    while True:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        data = response.json()

        if not data:
            print("No more data to retrieve.")
            break

        # Append the retrieved data to the collection
        all_data.extend(data)

        # Check if we have retrieved the last page
        if len(data) < params["pagesize"]:
            print("Last page of data retrieved.")
            break

        # Increment the page number for the next request
        params["page"] += 1

except requests.RequestException as e:
    print(f"An error occurred while making the API request: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Save the collected data to a CSV file
if all_data:
    try:
        df = pd.DataFrame(all_data)
        df.to_csv(output_file_path, index=False)
        print(f"Data successfully written to {output_file_path}")
    except Exception as e:
        print(f"An error occurred while writing data to CSV: {e}")
else:
    print("No data collected. CSV file will not be created.")
