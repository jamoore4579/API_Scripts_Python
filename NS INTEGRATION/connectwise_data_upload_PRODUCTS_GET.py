import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

# Define the endpoint and parameters for the request
endpoint = f"{BASE_URL}/procurement/catalog"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\CW_ALL_PRODUCTS_110624.csv"

# Initial parameters for pagination
params = {
    "conditions": "inactiveFlag = false",  # Filter by active products
    "fields": "id,identifier",  # Limit the fields returned to id and identifier
    "pagesize": 1000,
    "page": 1  # Start from the first page
}

all_data = []  # To collect all pages of data

# Loop through each page of results
while True:
    # Make the API request for the current page
    response = requests.get(endpoint, headers=headers, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # If no data is returned, break the loop
        if not data:
            break
        
        # Add current page data to the all_data list
        all_data.extend(data)
        
        # Move to the next page
        params["page"] += 1
    else:
        print(f"Failed to retrieve data: {response.status_code} - {response.text}")
        break

# Convert all collected data to a DataFrame and rename columns
if all_data:
    df = pd.DataFrame(all_data)
    # Rename 'id' to 'product_id' and 'identifier' to 'product_name' (assuming they exist)
    df = df.rename(columns={'id': 'product_id', 'identifier': 'product_name'})[['product_id', 'product_name']]
    
    # Append the DataFrame to the CSV file
    df.to_csv(output_file_path, mode='a', index=False, header=not os.path.exists(output_file_path))
    print(f"All product ID and Name data appended to {output_file_path}")
else:
    print("No data returned or failed to retrieve data.")
