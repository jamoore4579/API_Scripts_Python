import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Accessing API variables from the environment
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Define the endpoint for members with the required query parameters
MEMBERS_ENDPOINT = "/system/members?pagesize=1000&fields=firstName,lastName,vendorNumber&conditions=(inactiveFlag = false)"

# Set the headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Make a GET request to the ConnectWise API to pull all members
response = requests.get(BASE_URL + MEMBERS_ENDPOINT, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    members = response.json()
    
    # Load the members data into a DataFrame
    df = pd.DataFrame(members)
    
    # Define the file path for saving the data
    file_path = r"c:\users\jmoore\documents\connectwise\integration\connectwise_members.csv"
    
    # Save the DataFrame to a CSV file
    df.to_csv(file_path, index=False)
    
    print(f"Data saved successfully to {file_path}")
else:
    print(f"Failed to retrieve members. Status code: {response.status_code}, Response: {response.text}")
