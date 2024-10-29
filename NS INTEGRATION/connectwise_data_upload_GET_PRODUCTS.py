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
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\CW_ITEM_output_results_102924.csv"

params = {
    "conditions": "notes like 'NS Product Upload SB%' AND id >= 15000 AND id <= 18000",  # Filter by notes condition
    "pagesize": 1000
}

# Make the API request
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)
    
    # Append the DataFrame to the CSV file
    if not df.empty:
        df.to_csv(output_file_path, mode='a', index=False, header=not os.path.exists(output_file_path))
        print(f"Data appended to {output_file_path}")
    else:
        print("No data returned.")
else:
    print(f"Failed to retrieve data: {response.status_code} - {response.text}")