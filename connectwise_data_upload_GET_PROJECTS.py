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

# API headers
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# API endpoint
endpoint = f"{BASE_URL}/project/projects"

# Fetch all projects data
def get_projects():
    all_projects = []
    page = 1
    page_size = 100  # Adjust as needed

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "conditions": "((status/id=8 OR status/id=2) AND estimatedEnd>=[2025-02-01T00:00:00Z] AND estimatedEnd<=[2025-05-31T23:59:59Z])",
            "fields": "id,actualHours,status/name,percentComplete,estimatedEnd"
        }

        response = requests.get(endpoint, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_projects.extend(data)
            page += 1
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    return all_projects

# Retrieve data
projects = get_projects()

# Convert to DataFrame and clean up estimatedEnd
if projects:
    df = pd.DataFrame(projects)

    # Clean 'estimatedEnd' column: remove time part if it exists
    if 'estimatedEnd' in df.columns:
        df['estimatedEnd'] = df['estimatedEnd'].str.replace(r'T.*Z', '', regex=True)

    # Output file path
    output_path = r"C:\\users\\jmoore\\documents\\connectwise\\projects\\ProjectInfo052725.csv"
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"Data successfully saved to {output_path}")
else:
    print("No data retrieved.")
