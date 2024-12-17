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

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}"
}

# Function to fetch contacts for a specific company
def fetch_contacts(company_id):
    url = f"{BASE_URL}/company/contacts"
    params = {
        "conditions": f"(company/id = {company_id})",
        "fields": "id,firstName,lastName,company/name,defaultPhoneType,defaultPhoneNbr,communicationItems"
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# Main script logic
if __name__ == "__main__":
    input_file = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Contacts\\Contact_Company.csv"
    output_file = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Contacts\\CW_Contact_Data_121324.csv"

    all_contact_data = []

    try:
        # Load company IDs from the input file
        company_data = pd.read_csv(input_file)
        company_ids = company_data['CW_ID'].tolist()

        for company_id in company_ids:
            contacts = fetch_contacts(company_id)

            for contact in contacts:
                communication_items = contact.get("communicationItems", [])
                for comm_item in communication_items:
                    contact_data = {
                        "company_id": company_id,
                        "contact_id": contact.get("id", ""),
                        "first_name": contact.get("firstName", ""),
                        "last_name": contact.get("lastName", ""),
                        "company_name": contact.get("company", {}).get("name", f"Company_{company_id}"),
                        "default_phone_type": contact.get("defaultPhoneType", ""),
                        "default_phone_nbr": contact.get("defaultPhoneNbr", ""),
                        "communication_type": comm_item.get("type", ""),
                        "communication_value": comm_item.get("value", "")
                    }
                    all_contact_data.append(contact_data)

        # Save the data to a CSV file
        df = pd.DataFrame(all_contact_data)
        df.to_csv(output_file, index=False)
        print(f"Data successfully saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
