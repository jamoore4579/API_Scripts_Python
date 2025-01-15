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
    "Authorization": "Basic " + AUTH_CODE
}

# Define the API endpoint
endpoint = f"{BASE_URL}/company/companies"

# Import the file and retrieve columns CWID and NSID
input_file = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Company\\Production\\CompanyTaxStatus.csv"
try:
    missing_companies_df = pd.read_csv(input_file)
    cwid_nsid_pairs = missing_companies_df[["CWID", "NSID"]]
except Exception as e:
    print(f"Failed to read input file: {e}")
    exit(1)

# Initialize a list to store the retrieved data
retrieved_data = []

# Fetch data for each CWID from the API
for _, row in cwid_nsid_pairs.iterrows():
    cwid = row["CWID"]
    nsid = row["NSID"]
    
    params = {
        "fields": "id,name,status/name"
    }
    url = f"{endpoint}/{cwid}"

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        data["NSID"] = nsid  # Include NSID in the data for reference
        retrieved_data.append(data)
    else:
        print(f"Failed to retrieve data for CWID '{cwid}'. Status code: {response.status_code}, Response: {response.text}")

# Flatten the retrieved data
flattened_data = []
for entry in retrieved_data:
    flattened_entry = entry.copy()
    if "status" in entry and isinstance(entry["status"], dict):
        flattened_entry["status_name"] = entry["status"].get("name", "")
        del flattened_entry["status"]
    flattened_data.append(flattened_entry)

# Convert the flattened data to a Pandas DataFrame
df = pd.DataFrame(flattened_data)

# Define the output file path
output_file = r"C:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Company\\Production\\CW_Company_Data_010125.csv"

# Save the DataFrame to a CSV file
df.to_csv(output_file, index=False)
print(f"Data successfully saved to {output_file}")
