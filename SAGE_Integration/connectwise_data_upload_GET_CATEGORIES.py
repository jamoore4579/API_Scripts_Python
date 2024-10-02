import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Accessing API variables from the environment
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Define the endpoint for categories
CATEGORIES_ENDPOINT = "/procurement/categories"

# Set the headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Make a GET request to the ConnectWise API to pull all categories
response = requests.get(BASE_URL + CATEGORIES_ENDPOINT, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    categories = response.json()
    
    # Load the categories data into a DataFrame
    df = pd.DataFrame(categories)
    
    # Define the file path for saving the data
    file_path = r"c:\users\jmoore\documents\connectwise\integration\sandbox_categories.csv"
    
    # Save the DataFrame to a CSV file
    df.to_csv(file_path, index=False)
    
    print(f"Data saved successfully to {file_path}")
else:
    print(f"Failed to retrieve categories. Status code: {response.status_code}, Response: {response.text}")
