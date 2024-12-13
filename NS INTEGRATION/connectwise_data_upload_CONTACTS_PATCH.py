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

# Set up headers for the API requests
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2021.1"
}

# Load the CSV file paths
input_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Contacts\\NS_Contact_Update.csv"
output_file_path = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Contacts\\NS_Contacts_update.csv"

# List of required columns
required_columns = ['CW_ID']

# Load input data
df = pd.read_csv(input_file_path)

# Check if required columns exist
if not all(column in df.columns for column in required_columns):
    raise ValueError(f"Input file must contain the following columns: {required_columns}")

# Initialize results list
results = []

# Process the first five rows
first_batch = df.head(5)
for index, row in first_batch.iterrows():
    cw_id = row['CW_ID']

    # Perform company lookup
    company_url = f"{BASE_URL}/company/companies/{cw_id}"
    company_response = requests.get(company_url, headers=headers)

    if company_response.status_code == 200:
        company_data = company_response.json()
        
        # Perform contact lookup
        contact_url = f"{BASE_URL}/company/contacts"
        params = {
            "conditions": f"company/id={cw_id}",
            "childConditions": "types/id=16",
            "fields": "id"
        }
        contact_response = requests.get(contact_url, headers=headers, params=params)

        if contact_response.status_code == 200:
            contacts = contact_response.json()
            
            # Append data to results and update default contact
            for contact in contacts:
                contact_id = contact.get("id")
                
                # Update default contact
                patch_url = f"{BASE_URL}/company/companies/{cw_id}"
                patch_data = [
                    {
                        "op": "replace",
                        "path": "/defaultContact",
                        "value": {"id": contact_id}
                    }
                ]
                patch_response = requests.patch(patch_url, headers=headers, json=patch_data)

                if patch_response.status_code == 200:
                    print(f"Default contact updated for CW_ID {cw_id} with Contact_ID {contact_id}")
                else:
                    print(f"Failed to update default contact for CW_ID {cw_id}: {patch_response.text}")

                results.append({
                    "CW_ID": cw_id,
                    "Company_Name": company_data.get("name"),
                    "Contact_ID": contact_id
                })
        else:
            print(f"Failed to retrieve contacts for CW_ID {cw_id}: {contact_response.text}")
    else:
        print(f"Failed to retrieve company for CW_ID {cw_id}: {company_response.text}")

# Pause for confirmation to process remaining rows
proceed = input("Process remaining rows? (yes/no): ").strip().lower()
if proceed == "yes":
    remaining_batch = df.iloc[5:]
    for index, row in remaining_batch.iterrows():
        cw_id = row['CW_ID']

        # Perform company lookup
        company_url = f"{BASE_URL}/company/companies/{cw_id}"
        company_response = requests.get(company_url, headers=headers)

        if company_response.status_code == 200:
            company_data = company_response.json()
            
            # Perform contact lookup
            contact_url = f"{BASE_URL}/company/contacts"
            params = {
                "conditions": f"company/id={cw_id}",
                "childConditions": "types/id=16",
                "fields": "id"
            }
            contact_response = requests.get(contact_url, headers=headers, params=params)

            if contact_response.status_code == 200:
                contacts = contact_response.json()
                
                # Append data to results and update default contact
                for contact in contacts:
                    contact_id = contact.get("id")
                    
                    # Update default contact
                    patch_url = f"{BASE_URL}/company/companies/{cw_id}"
                    patch_data = [
                        {
                            "op": "replace",
                            "path": "/defaultContact",
                            "value": {"id": contact_id}
                        }
                    ]
                    patch_response = requests.patch(patch_url, headers=headers, json=patch_data)

                    if patch_response.status_code == 200:
                        print(f"Default contact updated for CW_ID {cw_id} with Contact_ID {contact_id}")
                    else:
                        print(f"Failed to update default contact for CW_ID {cw_id}: {patch_response.text}")

                    results.append({
                        "CW_ID": cw_id,
                        "Company_Name": company_data.get("name"),
                        "Contact_ID": contact_id
                    })
            else:
                print(f"Failed to retrieve contacts for CW_ID {cw_id}: {contact_response.text}")
        else:
            print(f"Failed to retrieve company for CW_ID {cw_id}: {company_response.text}")

# Create a DataFrame from results
results_df = pd.DataFrame(results)

# Save results to output CSV
results_df.to_csv(output_file_path, index=False)

print(f"Contact lookup and update completed. Results saved to {output_file_path}.")
