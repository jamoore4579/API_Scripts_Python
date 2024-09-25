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

def get_companies_by_name(name_segment):
    try:
        # Use the name_segment to filter companies via API (name like %name_segment%)
        params = {
            "pageSize": 1000,
            "conditions": f"(name like '%{name_segment}%')",  # Dynamic condition using name_segment
            "fields": "id,identifier,name"  # Only retrieve id, identifier, and name fields
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Check if the response is a list
        response_data = response.json()
        if isinstance(response_data, list):
            # If the response is a list, treat it as the company data
            return response_data
        else:
            print("Unexpected response format.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching companies from API: {e}")
        return None

def upload_and_compare_company_data():
    # Load the customer data from the CSV file
    customer_file_path = r'c:\users\jmoore\documents\connectwise\projects\All_Customers_Data.csv'
    customer_df = pd.read_csv(customer_file_path)

    # Extract the first, second, and third segments of the 'Name' column, if available
    def extract_name_segments(name):
        segments = name.split()
        return segments[:3]  # Return the first three segments (or fewer if there are less)

    # Apply the function to extract name segments
    customer_df['Name_Segments'] = customer_df['Name'].apply(extract_name_segments)

    # Create DataFrames to store matched and unmatched customers
    matched_customers = pd.DataFrame()
    unmatched_customers = pd.DataFrame()

    # Loop through each customer and fetch companies that match
    for index, row in customer_df.iterrows():
        name_segments = row['Name_Segments']
        is_matched = False  # Flag to check if customer was matched

        for segment in name_segments:  # Loop through each segment (first, second, third) of the name
            print(f"Fetching companies for name segment: {segment}")
            matched_companies = get_companies_by_name(segment)

            if matched_companies:
                # If we found matching companies, append this customer to the matched customers DataFrame using pd.concat
                matched_customers = pd.concat([matched_customers, pd.DataFrame([row])], ignore_index=True)
                is_matched = True
                break  # Stop searching once a match is found for this customer

        if not is_matched:
            # If no match was found, append this customer to the unmatched customers DataFrame
            unmatched_customers = pd.concat([unmatched_customers, pd.DataFrame([row])], ignore_index=True)

    # Log the matched customers
    if not matched_customers.empty:
        matched_file_path = r'c:\users\jmoore\documents\connectwise\projects\Matched_Customers.csv'
        matched_customers.to_csv(matched_file_path, index=False)
        print(f"Matched customers logged and saved to '{matched_file_path}'.")

    # Log the unmatched customers
    if not unmatched_customers.empty:
        unmatched_file_path = r'c:\users\jmoore\documents\connectwise\projects\Unmatched_Customers.csv'
        unmatched_customers.to_csv(unmatched_file_path, index=False)
        print(f"Unmatched customers logged and saved to '{unmatched_file_path}'.")
    else:
        print("No unmatched customers found.")

# Call the function to load and compare company data
upload_and_compare_company_data()
