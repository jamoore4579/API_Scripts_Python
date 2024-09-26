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

# Function to get companies from the Connectwise API based on company name segment
def get_companies_by_name_segment(name_segment):
    try:
        # Construct conditions to search for companies with names starting with name_segment
        conditions = f'(name like "{name_segment}%")'
        
        # Parameters for filtering the API response
        params = {
            "pageSize": 1000,
            "conditions": conditions,  # Search for companies whose name starts with name_segment
            "fields": "id,identifier,name,addressLine1,city,state,zip,phoneNumber,territory,name,dateAcquired,billingTerms"
        }
        
        # Make the API request
        response = requests.get(url=f"{BASE_URL}/company/companies", headers=headers, params=params)
        response.raise_for_status()

        # Parse the response data
        response_data = response.json()
        if not isinstance(response_data, list):
            print("Unexpected response format.")
            return None

        return pd.DataFrame(response_data)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching companies from API: {e}")
        return None

# Function to split and process the company name into segments
def extract_name_segments(name):
    segments = name.split()
    return " ".join(segments[:3]) if len(segments) >= 3 else name

# Function to load customer data from the CSV file
def load_customer_data(file_path):
    try:
        customer_data = pd.read_csv(file_path)
        customer_data['Name_Segments'] = customer_data['Name'].apply(extract_name_segments)
        return customer_data
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

# Function to match customer names with Connectwise company names
def match_companies(customers_df):
    matches = []

    for _, customer in customers_df.iterrows():
        customer_name_segments = customer['Name_Segments']
        
        # Fetch companies that match the customer name segment from the API
        companies_df = get_companies_by_name_segment(customer_name_segments)
        if companies_df is not None and not companies_df.empty:
            match = companies_df.iloc[0]  # Taking the first match, modify this logic if needed
            
            # Extract nested fields safely
            territory_name = match.get('territory', {}).get('name', None)
            billing_terms_name = match.get('billingTerms', {}).get('name', None)
            
            matches.append((
                customer['Name'], 
                customer_name_segments, 
                match.get('name', None), 
                match.get('id', None), 
                match.get('identifier', None),
                match.get('addressLine1', None), 
                match.get('city', None), 
                match.get('state', None), 
                match.get('zip', None), 
                match.get('phoneNumber', None), 
                territory_name,  # Extracted territory name
                match.get('dateAcquired', None), 
                billing_terms_name  # Extracted billing terms
            ))
        else:
            matches.append((
                customer['Name'], customer_name_segments, "No Match Found", None, None, None, None, None, None, None, None, None, None
            ))

    return pd.DataFrame(matches, columns=['Original Name', 'Extracted Segments', 'Matched Company', 'Company ID', 'Company Identifier', 'addressLine1', 'city', 'state', 'zip', 'phoneNumber', 'territory/name', 'dateAcquired', 'billingTerms/name'])

# Function to save the matched data to a CSV file
def save_matches_to_csv(matched_df, output_file_path):
    try:
        matched_df.to_csv(output_file_path, index=False)
        print(f"Matched data written to '{output_file_path}'.")
    except Exception as e:
        print(f"Error writing the file: {e}")

# Function to upload the customer data, match against Connectwise companies, and save to CSV
def upload_and_match_customer_data():
    customer_file_path = r'c:\users\jmoore\documents\connectwise\projects\Matched_Customers_Information.csv'
    output_file_path = r'c:\users\jmoore\documents\connectwise\projects\Matched_Company_Results.csv'
    
    # Load customer data from CSV
    customers_df = load_customer_data(customer_file_path)
    if customers_df is None:
        print("No customer data to process.")
        return

    # Match the customer data with companies
    matched_df = match_companies(customers_df)
    
    # Save the results to CSV
    save_matches_to_csv(matched_df, output_file_path)

# Call the function to load, match, and save customer data
upload_and_match_customer_data()
