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
    "Authorization": "Basic " + AUTH_CODE
}

def get_glAccounts():
    try:
        all_glAccounts = []  # Store all retrieved glAccounts
        page = 1  # Start with the first page

        while True:
            # Updated parameters, supporting pagination
            params = {
                "conditions": "(glType = 'EI')",
                "page": page  # Current page number
            }

            # Make the API request
            response = requests.get(f"{BASE_URL}/finance/glAccounts", headers=headers, params=params)
            response.raise_for_status()

            # Parse the JSON response
            response_data = response.json()

            if not isinstance(response_data, list) or not response_data:
                break  # Exit loop if no more data

            # Normalize data and extract required fields
            for glAccounts in response_data:
                all_glAccounts.append({
                    "id": glAccounts.get("id", "Unknown"),
                    "glType": glAccounts.get("glType", "Unknown"),
                    "mappedRecord": glAccounts.get("mappedRecord", {}).get("name", "Unknown")
                })

            page += 1  # Move to the next page

        # Convert all data to a DataFrame
        results_df = pd.DataFrame(all_glAccounts)

        # Specify desired column order
        column_order = [
            "id", "glType", "mappedRecord"
        ]

        # Reindex DataFrame to ensure column order
        results_df = results_df.reindex(columns=column_order)

        return results_df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching glAccounts from API: {e}")
        return None

def write_glAccounts_to_csv(dataframe, file_path):
    try:
        # Write all data to the specified CSV file, overwriting if the file exists
        dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"glAccount data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def upload_glAccounts_data():
    glAccounts_df = get_glAccounts()
    if glAccounts_df is not None:
        results_file_path = (
            r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\CW_glAccounts_Data_112124.csv'
        )
        write_glAccounts_to_csv(glAccounts_df, results_file_path)
    else:
        print("No data to write to CSV.")

# Execute the function to retrieve and upload company data
upload_glAccounts_data()
