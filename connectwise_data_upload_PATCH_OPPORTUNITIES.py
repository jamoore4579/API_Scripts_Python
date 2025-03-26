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

# Setup headers for API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Path to the CSV file
csv_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\Opportunity\\forecastnotes.csv"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\Opportunity\\forecastnoteoutput.csv"

# Read the CSV file
data = pd.read_csv(csv_file_path)

# Check if the required columns exist
if 'CWID' not in data.columns or 'NoteValue' not in data.columns:
    raise ValueError("The CSV file must contain 'CWID' and 'NoteValue' columns.")

# Function to process records
def process_records(records):
    responses = []
    for index, row in records.iterrows():
        opp_id = int(row['CWID']) if not pd.isna(row['CWID']) else None
        note_value = row['NoteValue'] if not pd.isna(row['NoteValue']) else None  # Directly pulling NoteValue from the file

        if opp_id is None or note_value is None:
            print(f"Skipping record with missing data at index {index}")
            continue

        # Construct PATCH request data
        patch_data = [
            {
                "op": "replace",
                "path": "/customFields",
                "value": [{"id": 18, "value": note_value}]
            }
        ]

        # Define the API endpoint
        endpoint = f"{BASE_URL}/sales/opportunities/{opp_id}"

        try:
            # Make the PATCH request
            response = requests.patch(endpoint, headers=headers, json=patch_data)
            
            # Log the response
            print(f"Processed OPP_ID: {opp_id} with CustomField: {note_value}")
            responses.append({
                "OPP_ID": opp_id,
                "NoteValue": note_value,
                "Status": response.status_code,
                "Response": response.text
            })
        except requests.exceptions.RequestException as e:
            print(f"Error processing OPP_ID: {opp_id}. Error: {e}")
            responses.append({
                "OPP_ID": opp_id,
                "NoteValue": note_value,
                "Status": "Error",
                "Response": str(e)
            })

    return responses

# Process the first five records
first_five = data.head(5)
responses = process_records(first_five)

# Output responses for the first five records
output_data = pd.DataFrame(responses)
output_data.to_csv(output_file_path, index=False)

for resp in responses:
    print(resp)

# Ask user to proceed with the rest of the records
proceed = input("Do you want to process the remaining records? (yes/no): ").strip().lower()
if proceed == 'yes':
    remaining_records = data.iloc[5:]
    responses = process_records(remaining_records)

    # Append responses for the remaining records
    output_data = pd.DataFrame(responses)
    output_data.to_csv(output_file_path, mode='a', index=False, header=False)

    for resp in responses:
        print(resp)
else:
    print("Processing halted by the user.")
