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
file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\CW_OPP_output_results_102724.csv"
data = pd.read_csv(file_path)

# Initialize list to keep track of deletion results
results = []

# Iterate through each row in the dataframe and delete contacts using the ConnectWise API
for index, row in data.iterrows():
    opp_id = row['ID']
    delete_url = f"{BASE_URL}/sales/opportunities/{opp_id}"

    # Make the DELETE request
    response = requests.delete(delete_url, headers=headers)

    # Record the result
    result = {
        "conID": opp_id,
        "Status Code": response.status_code,
        "Response": response.text
    }
    results.append(result)
    print(f"Deleted opportunity {opp_id}: Status {response.status_code}")

# Convert the results list to a DataFrame
results_df = pd.DataFrame(results)

# Save the results to a new CSV file
output_file_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\opportunity_deleted_102724.csv"
results_df.to_csv(output_file_path, index=False)

print(f"Deletion results saved to {output_file_path}")
