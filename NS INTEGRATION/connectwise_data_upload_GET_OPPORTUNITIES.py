import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_SAND")
AUTH_CODE = os.getenv("AUTH_SAND")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json"
}

# Define the URL and initial parameters with additional conditions for ID range
url = f"{BASE_URL}/sales/opportunities"
params = {
    "customFieldConditions": 'caption="Netsuite ID" AND value != null',
    "conditions": 'id >= 18000 AND id <= 28000',  # Added ID range condition
    "pageSize": 1000  # Maximum allowed size
}

all_data = []  # To store all opportunities data
more_pages = True  # Loop control variable

while more_pages:
    # Make the GET request to the API
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        opportunities = response.json()  # Parse the response JSON

        # If no opportunities are returned, stop the loop
        if not opportunities:
            more_pages = False
            break

        # Extract the required fields: id, name, and Netsuite ID
        for opp in opportunities:
            opp_id = opp.get('id')
            opp_name = opp.get('name')
            netsuite_id = None

            # Loop through custom fields to find the "Netsuite ID"
            custom_fields = opp.get('customFields', [])
            for field in custom_fields:
                if field.get('caption') == 'Netsuite ID' and field.get('value') is not None:
                    netsuite_id = field.get('value')
                    break

            if netsuite_id:  # Only add to the data if Netsuite ID is found
                all_data.append({
                    'ID': opp_id,
                    'Name': opp_name,
                    'Netsuite ID': netsuite_id
                })

        # Check if more pages are available (assuming the API includes a paging mechanism)
        if 'Link' in response.headers:
            # Look for a "next" page link in the headers
            link_header = response.headers['Link']
            if 'rel="next"' in link_header:
                # Parse the "next" page URL (modify as needed for your API format)
                next_url = link_header.split(';')[0].strip('<>')
                url = next_url  # Update the URL for the next request
            else:
                more_pages = False  # No more pages
        else:
            more_pages = False  # No pagination found
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}, Response: {response.text}")
        break

# Convert the data to a DataFrame
df = pd.DataFrame(all_data)

# Output the DataFrame to a CSV file (append mode)
output_file = r'c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\CW_OPP_output_results_102724.csv'

# Append the data to the CSV file
df.to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))

print(f"Data successfully appended to {output_file}. Total records appended: {len(df)}")
