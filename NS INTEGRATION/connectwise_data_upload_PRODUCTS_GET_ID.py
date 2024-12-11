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

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

# File paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\Product_ID.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\Product_ID_Results.csv"

# Read the input file
try:
    product_data = pd.read_csv(input_file_path)
    if "ProductID" not in product_data.columns:
        raise ValueError("The input file must contain a 'ProductID' column.")
except Exception as e:
    print(f"Error reading input file: {e}")
    exit()

# Initialize a list to hold results
results = []

# Loop through each ProductID and make the API request
for product_id in product_data["ProductID"]:
    try:
        # Define endpoint and parameters
        endpoint = f"{BASE_URL}/procurement/catalog"
        params = {"conditions": f"identifier like '{product_id}'"}
        
        # Make the GET request
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Extract the ID from the response
        response_data = response.json()
        if response_data and isinstance(response_data, list) and len(response_data) > 0:
            item_id = response_data[0].get("id", "No ID Found")
        else:
            item_id = "No Data Returned"
        
        # Append the result
        results.append({"ProductID": product_id, "ID": item_id})
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for ProductID {product_id}: {e}")
        results.append({"ProductID": product_id, "ID": "Error"})

# Save the results to the output file
try:
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_file_path, index=False)
    print(f"Results successfully saved to {output_file_path}")
except Exception as e:
    print(f"Error writing results to file: {e}")
