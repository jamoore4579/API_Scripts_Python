import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load API credentials and base URL from environment variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# File paths for input and output
csv_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\SalesOrders\\NS_Item_SalesOrders_122924.csv"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\SalesOrders\\NS_Item_SalesOrders_122924_Results.csv"

# Specify columns to read
columns_to_read = [
    'CWID', 'Item', 'Quantity', 'CustRate', 'Cost'
]

# Load the CSV file, handling missing columns gracefully
try:
    contacts_df = pd.read_csv(csv_file_path, usecols=columns_to_read)
except ValueError as e:
    print(f"Warning: {e}")
    contacts_df = pd.read_csv(csv_file_path)
    for col in columns_to_read:
        if col not in contacts_df.columns:
            contacts_df[col] = ''

# Initialize results list
results = []

# Flag to determine whether to pause after the first prompt
pause_after_first_prompt = True

# Process the records
for index, row in contacts_df.iterrows():
    # Extract values from the current row
    cwid = row.get('CWID')
    item = row.get('Item')
    quantity = row.get('Quantity')
    cust_rate = row.get('CustRate')
    cost = row.get('Cost')

    # Prepare the payload for the API request
    payload = {
        "catalogItem": {
            "identifier": f"{item}"
        },
        "quantity": quantity,
        "price": cust_rate,
        "cost": cost,
        "billableOption": "Billable",
        "salesOrder": {
            "id": cwid
        }
    }

    # Construct the API URL
    url = f"{BASE_URL}/procurement/products"

    try:
        # Send the POST request
        response = requests.post(url, headers=headers, json=payload)
        response_data = {
            "CWID": cwid,
            "Status Code": response.status_code,
            "Response": response.json() if response.status_code == 200 else response.text
        }
        print(response_data)
        results.append(response_data)
    except Exception as e:
        error_data = {
            "CWID": cwid,
            "Status Code": "Error",
            "Response": str(e)
        }
        print(error_data)
        results.append(error_data)

    # Pause after every 5 records
    if (index + 1) % 5 == 0 and pause_after_first_prompt:
        proceed = input("Processed 5 records. Do you want to continue? (yes/no): ").strip().lower()
        if proceed != 'yes':
            break
        pause_after_first_prompt = False  # Disable further pauses

# Save the results to the output file
results_df = pd.DataFrame(results)
results_df.to_csv(output_file_path, index=False)

print(f"Results saved to {output_file_path}")
