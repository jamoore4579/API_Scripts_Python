import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Validate environment variables
if not all([BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing one or more environment variables.")

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# File paths
input_path = r"C:\users\jmoore\documents\connectwise\Company\DuplicateCheck.csv"
output_path = r"C:\users\jmoore\documents\connectwise\Company\DuplicateCheckResults.csv"

# Read input file
df_input = pd.read_csv(input_path)

# Results list
results = []

# Process each ConnectID
for _, row in df_input.iterrows():
    connectid = row.get('ConnectwiseID')
    if pd.isna(connectid):
        results.append({"ConnectID": connectid, "ID": "Not Found", "FirstName": "Not Found", "LastName": "Not Found"})
        continue

    url = f"{BASE_URL}/company/contacts/{int(connectid)}"
    params = {"fields": "id,firstName,lastName,company/name"}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        results.append({
            "ConnectID": connectid,
            "ID": data.get("id", ""),
            "FirstName": data.get("firstName", ""),
            "LastName": data.get("lastName", "")
        })
    elif response.status_code == 404:
        results.append({"ConnectID": connectid, "ID": "Not Found", "FirstName": "Not Found", "LastName": "Not Found"})
    else:
        results.append({"ConnectID": connectid, "ID": f"Error {response.status_code}", "FirstName": "", "LastName": ""})

# Convert results to DataFrame and save to CSV
df_output = pd.DataFrame(results)
df_output.to_csv(output_path, index=False)

print(f"Results written to {output_path}")
