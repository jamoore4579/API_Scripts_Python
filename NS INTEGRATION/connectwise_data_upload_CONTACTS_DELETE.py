import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Load the CSV file containing contact IDs
file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Delete_Contacts.csv"
data = pd.read_csv(file_path)

# Initialize list to keep track of deletion results
results = []

# Iterate through each row in the dataframe and delete contacts using the ConnectWise API
for index, row in data.iterrows():
    contact_id = row['conID']
    delete_url = f"{BASE_URL}/company/contacts/{contact_id}"

    # Make the DELETE request
    response = requests.delete(delete_url, headers=headers)

    # Record the result
    result = {
        "conID": contact_id,
        "Status Code": response.status_code,
        "Response": response.text
    }
    results.append(result)
    print(f"Deleted contact {contact_id}: Status {response.status_code}")

# Convert the results list to a DataFrame
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\contact_deleted.csv"
results_df.to_csv(output_file_path, index=False)

print(f"Deletion results saved to {output_file_path}")
