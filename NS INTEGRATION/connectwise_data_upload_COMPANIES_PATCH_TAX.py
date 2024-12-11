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

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# File paths
input_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Taxable_Customers.csv"
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\Taxable_Company_Update.csv"

# Read CSV file
df = pd.read_csv(input_file_path)

# Adding new columns to store the status of each API request
df['API_Status'] = ''  # To capture if the API call was successful or not
df['API_Response'] = ''  # To capture the response message

# Iterate through the DataFrame rows
for index, row in df.iterrows():
    try:
        # Check if Taxable value is 'no'
        if row['Taxable'].strip().lower() == 'no':
            cw_id = row['CW_ID']  # Get the CW_ID value

            # Define the PATCH payload
            payload = [
                {
                    "op": "replace",
                    "path": "/companyEntityType",
                    "value": {"id": 5 }
                }
            ]

            # Perform the PATCH request
            response = requests.patch(
                f"{BASE_URL}/company/companies/{cw_id}",
                headers=headers,
                json=payload
            )

            # Update the DataFrame with API results
            df.at[index, 'API_Status'] = 'Success' if response.status_code == 200 else 'Failed'
            df.at[index, 'API_Response'] = response.text
        else:
            df.at[index, 'API_Status'] = 'Skipped'
            df.at[index, 'API_Response'] = 'Non-taxable customer'
    except Exception as e:
        # Handle any exceptions and log the error in the DataFrame
        df.at[index, 'API_Status'] = 'Error'
        df.at[index, 'API_Response'] = str(e)

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file_path, index=False)

