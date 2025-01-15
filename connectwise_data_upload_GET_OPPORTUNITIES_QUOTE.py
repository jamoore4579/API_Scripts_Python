import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
SELL_URL = os.getenv("SELL_URL")
AUTH_ID = os.getenv("AUTH_ID")

# Setup headers for API requests
headers = {
    "Authorization": "Basic " + AUTH_ID,
    "Content-Type": "application/json"
}

# Define the API endpoint
endpoint = f"{SELL_URL}/quotes"

# Initialize an empty list to store filtered records
filtered_records = []

# Paginate through the results
page = 1
page_size = 100
while True:
    params = {
        'page': page,
        'pageSize': page_size,
        'conditions': 'crmOpportunityId!=NULL'
    }
    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}, Response: {response.text}")
        break

    data = response.json()

    # Stop the loop if no more data is found
    if not data:
        break

    # Add the filtered records to the list
    filtered_records.extend(data)
    
    page += 1

# Create a DataFrame from the filtered records
df = pd.DataFrame(filtered_records)

# Define the output file path
output_file = r"c:\\users\\jmoore\\documents\\connectwise\\Integration\\NS_Integration\\Opportunity\\Production\\FilteredData.csv"

# Save the DataFrame to a CSV file
df.to_csv(output_file, index=False)

print(f"Data successfully saved to {output_file}")
