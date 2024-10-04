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

# Prepare headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load the CSV file
input_file = r"c:\users\jmoore\documents\connectwise\integration\Keep_Products_updated_ID.csv"
output_file = r"c:\users\jmoore\documents\connectwise\integration\product_CAT_updates.csv"

# Read the input CSV file
df = pd.read_csv(input_file)

# Columns expected: 'id', 'Category', 'Subcategory'
# Initialize an empty list to store the results
results = []

# Iterate through each row and send a PATCH request
for index, row in df.iterrows():
    product_id = row['id']
    category_id = row['Category']
    subcategory_id = row['Subcategory']
    
    # API endpoint
    url = f"{BASE_URL}/procurement/catalog/{product_id}"
    
    # Data payload
    payload = [
        {
            "op": "replace",
            "path": "category",
            "value": {
                "id": category_id
            }
        },
        {
            "op": "replace",
            "path": "subcategory",
            "value": {
                "id": subcategory_id
            }
        }
    ]

    # Make the PATCH request
    response = requests.patch(url, json=payload, headers=headers)

    # Check the response
    if response.status_code == 200:
        result_status = "Success"
    else:
        result_status = f"Failed - {response.status_code}: {response.text}"
    
    # Append result to the results list
    results.append({
        "Product ID": product_id,
        "Category ID": category_id,
        "Subcategory ID": subcategory_id,
        "Status": result_status
    })

# Convert the results list to a DataFrame
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
results_df.to_csv(output_file, index=False)

print(f"Process completed. Results have been saved to {output_file}")
