import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables from environment variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load CSV file
file_path = r"c:\users\jmoore\documents\connectwise\integration\remove_contacts.csv"
df = pd.read_csv(file_path)

# Extract the required columns and handle missing values
# Ensure that 'ContactID' column is in the CSV for correct processing
contact_data = df[['ContactID']].dropna()

# Create a list to store results of deletion operations
results = []

# Loop through the contact data to perform delete operation
for index, row in contact_data.iterrows():
    contact_id = row['ContactID']

    # Construct the URL for the DELETE request using only ContactID
    delete_url = f"{BASE_URL}/company/contacts/{contact_id}"

    try:
        # Make DELETE request to the ConnectWise API
        response = requests.delete(delete_url, headers=headers)

        # Check if the deletion was successful
        if response.status_code == 200 or response.status_code == 204:
            result = {
                "ContactID": contact_id,
                "Status": "Deleted"
            }
        else:
            result = {
                "ContactID": contact_id,
                "Status": f"Failed - {response.status_code}: {response.text}"
            }

    except Exception as e:
        # If an error occurs, capture it in the results
        result = {
            "ContactID": contact_id,
            "Status": f"Error - {str(e)}"
        }

    # Append the result of each operation to the results list
    results.append(result)

# Convert the results list to a DataFrame
results_df = pd.DataFrame(results)

# Write the results DataFrame to a new CSV file
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\contact_deletion_results.csv"
results_df.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")
