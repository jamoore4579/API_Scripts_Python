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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the initial URL and parameters
url = f"{BASE_URL}/sales/opportunities"
params = {
    "conditions": 'id >= 19848 AND id <= 20850',  # Modify or remove condition as needed
    "pageSize": 1000,  # Maximum allowed size
    "page": 1  # Start with the first page
}

all_data = []  # To store filtered opportunities data

while True:
    # Make the GET request to the API
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        opportunities = response.json()  # Parse the response JSON

        # If no opportunities are returned, stop the loop
        if not opportunities:
            break

        # Filter the data to include only `id` and customField with `id` 75
        for opp in opportunities:
            custom_field_75 = next((field["value"] for field in opp.get("customFields", []) if field["id"] == 75), None)
            all_data.append({
                "id": opp.get("id"),
                "customField_75": custom_field_75
            })

        # Increment the page number for the next request
        params["page"] += 1
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}, Response: {response.text}")
        break

# Convert the filtered data to a DataFrame
df = pd.DataFrame(all_data)

# Output the DataFrame to a CSV file (append mode)
output_file = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\Production\CW_OPP_output_results_122824.csv'

# Append the data to the CSV file
df.to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))

print(f"Data successfully appended to {output_file}. Total records appended: {len(df)}")
