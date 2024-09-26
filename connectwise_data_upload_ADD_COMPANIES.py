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

def read_companies_from_csv(file_path):
    try:
        # Read the CSV file into a DataFrame
        companies_df = pd.read_csv(file_path)
        return companies_df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def create_company_in_connectwise(company_data):
    try:
        # Prepare the payload for the API request
        payload = {
            "name": company_data['Name'],
            "identifier": company_data['Identifier'],
            "addressLine1": company_data['addressLine1'],
            "city": company_data['city'],
            "state": company_data['state'],
            "zip": company_data['zip'],
            "phoneNumber": company_data['phoneNumber'],
            "territory": company_data['territory'],
            "category": company_data['Category']
        }

        # Make the API request to create the company
        response = requests.post(url=f"{BASE_URL}/company/companies", headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for any HTTP errors

        # Return the result as a dictionary
        return {"status": "success", "company": company_data['Name'], "response": response.json()}

    except requests.exceptions.RequestException as e:
        # Return failure details in case of error
        return {"status": "failed", "company": company_data['Name'], "error": str(e)}

def upload_companies_from_csv():
    # Path to the CSV file
    csv_file_path = r'c:\users\jmoore\documents\connectwise\projects\Upload_Customers.csv'

    # Read the company data from CSV
    companies_df = read_companies_from_csv(csv_file_path)

    # List to store results of each company creation attempt
    creation_results = []

    # If data exists, proceed with creating companies
    if companies_df is not None:
        for _, company in companies_df.iterrows():
            # Create each company in ConnectWise and capture the result
            result = create_company_in_connectwise(company)
            creation_results.append(result)
            
            # Print out the result of each creation attempt
            if result['status'] == "success":
                print(f"Successfully created company: {company['Name']}")
            else:
                print(f"Failed to create company: {company['Name']}, Error: {result['error']}")
    else:
        print("No data available from CSV.")

    # Return the list of results
    return creation_results

# Call the function to upload company data from CSV to ConnectWise and get the results
results = upload_companies_from_csv()

# Optionally, you can further process or save the results
# For example, save results to a file
results_file_path = r'c:\users\jmoore\documents\connectwise\projects\creation_results.csv'
results_df = pd.DataFrame(results)
results_df.to_csv(results_file_path, index=False)

print(f"Results of company creation attempts written to '{results_file_path}'.")
