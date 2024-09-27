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
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Function to read the customer data from the CSV file
def load_customer_data(csv_file_path):
    try:
        # Read the CSV file into a DataFrame
        customer_data = pd.read_csv(csv_file_path)
        
        # Check if required columns exist
        if 'ConnectwiseID' not in customer_data.columns or 'TypeId' not in customer_data.columns:
            print("The CSV file must contain 'ConnectwiseID' and 'TypeId' columns.")
            return None
        
        return customer_data
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

# Function to update company types via POST request
def post_company_type(connectwise_id, type_id):
    try:
        # Convert to native Python int type
        connectwise_id = int(connectwise_id)
        type_id = int(type_id)
        
        # Prepare the data for the POST request
        data = {
            "company": {"id": connectwise_id},  # Use the ConnectwiseID from the CSV file
            "type": {"id": type_id}  # Use the TypeId from the CSV file
        }
        
        # Make the POST request to update the company type
        url = f"https://psa.endeavorit.com/v4_6_release/apis/3.0/company/companytypeAssociations"
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an exception for HTTP errors

        print(f"Successfully associated company {connectwise_id} with TypeId {type_id}.")
    except requests.exceptions.RequestException as e:
        print(f"Error updating company {connectwise_id}: {e}")

# Function to process the customer data and make POST requests
def process_and_post_companies(csv_file_path):
    # Load the customer data from the CSV file
    customer_data = load_customer_data(csv_file_path)

    # If data is successfully loaded, process each row
    if customer_data is not None:
        for index, row in customer_data.iterrows():
            connectwise_id = row['ConnectwiseID']
            type_id = row['TypeId']

            # Post the company type association via POST request
            post_company_type(connectwise_id, type_id)

# Call the function to process and post companies
csv_file_path = r'c:\users\jmoore\documents\connectwise\projects\Customer_Type.csv'
process_and_post_companies(csv_file_path)
