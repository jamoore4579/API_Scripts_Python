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
    "Authorization": "Basic " + AUTH_CODE
}

# Define file paths
DELETE_FILE_PATH = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\delete_companies.csv'
RESULTS_FILE_PATH = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Company\delete_company_data_results.csv'

def read_company_ids(file_path):
    """
    Reads company IDs from the given CSV file.
    """
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)

        # Check if the 'id' column exists in the file
        if 'id' in df.columns:
            return df['id'].tolist()
        else:
            print(f"No 'id' column found in the file: {file_path}")
            return []

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error reading the file: {e}")
        return []

def delete_company(company_id):
    """
    Sends a DELETE request to the ConnectWise API to delete a company by ID.
    """
    try:
        # Construct the API endpoint URL
        url = f"{BASE_URL}/company/companies/{company_id}"

        # Make the DELETE request
        response = requests.delete(url=url, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Return success message with the status code
        return {"id": company_id, "status": "Deleted", "message": f"Company with ID {company_id} deleted successfully."}

    except requests.exceptions.RequestException as e:
        # Return error message with the status code
        return {"id": company_id, "status": "Failed", "message": str(e)}

def log_deletion_results(results, file_path):
    """
    Writes the deletion results to a CSV file.
    """
    try:
        # Convert the results list to a DataFrame
        results_df = pd.DataFrame(results)

        # Write the results to a CSV file
        results_df.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Deletion results written to '{file_path}'.")

    except Exception as e:
        print(f"Error writing results to file: {e}")

def delete_companies_from_file(delete_file_path, results_file_path):
    """
    Reads company IDs from a file and deletes them using the ConnectWise API.
    Logs the results to a new file.
    """
    # Step 1: Read company IDs from the file
    company_ids = read_company_ids(delete_file_path)

    # If no company IDs found, print a message and exit
    if not company_ids:
        print("No company IDs found to delete.")
        return

    # Step 2: Delete each company by ID and collect the results
    deletion_results = []
    for company_id in company_ids:
        result = delete_company(company_id)
        deletion_results.append(result)
        print(result["message"])  # Print each result as it's processed

    # Step 3: Log the deletion results to a CSV file
    log_deletion_results(deletion_results, results_file_path)

# Call the function to delete companies and log the results
delete_companies_from_file(DELETE_FILE_PATH, RESULTS_FILE_PATH)
