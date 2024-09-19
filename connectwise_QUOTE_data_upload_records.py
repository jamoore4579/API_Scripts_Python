import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
SELL_URL = os.getenv("SELL_URL")
AUTH_ID = os.getenv("AUTH_ID")

# Quote Templates
REGULAR_QUOTE_TEMPLATE_ID = "q638615886842932838rVUfSEv"
ERATE_QUOTE_TEMPLATE_ID = "q638615888842148413ohzNAvc"
RENEWAL_QUOTE_TEMPLATE_ID = "q638615890384714830mlNHtxm"

# Headers for requests
headers = {
    "Authorization": "Basic " + AUTH_ID,
    "Content-Type": "application/json"
}

def post_quote_by_template(template_id, quote_data, quote_template_name):
    try:
        # Post the quote to the specified endpoint
        response = requests.post(url=f"{SELL_URL}/quotes/copyById/{template_id}", headers=headers, json=quote_data)
        response.raise_for_status()

        # Get the quote ID from the response (assuming it's in JSON format)
        quote_id = response.json().get("id")
        print(f"Quote added successfully: {quote_template_name}, Quote ID: {quote_id}")
        
        return quote_id
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
    return None

def patch_quote(quote_id, title, document_number):
    patch_data = [
        {
            "value": f"{title}",
            "path": "/name",
            "op": "add"
        },
        {
            "value": f"{document_number}",
            "path": "/originalQuoteId",
            "op": "add"
        }
    ]
    
    try:
        # Send the PATCH request to update the quote name and originalQuoteId
        patch_response = requests.patch(url=f"{SELL_URL}/quotes/{quote_id}", headers=headers, json=patch_data)
        patch_response.raise_for_status()
        print(f"Quote updated successfully for Quote ID: {quote_id}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error (PATCH): {errh}")
    except requests.exceptions.RequestException as err:
        print(f"Request Error (PATCH): {err}")

def process_csv_and_post_quotes(file_path):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return
    except pd.errors.EmptyDataError:
        print(f"File is empty: {file_path}")
        return
    except pd.errors.ParserError:
        print(f"File format not supported or corrupted.")
        return

    if "Custom_Form" not in df.columns or "Title" not in df.columns or "Document_Number" not in df.columns:
        print("The 'Custom_Form', 'Title', or 'Document_Number' column is missing in the CSV file.")
        return

    for index, row in df.iterrows():
        custom_form_value = row.get("Custom_Form", "")
        title = row.get("Title", "")
        document_number = row.get("Document_Number", "")

        quote_data = {
            "details": row.to_dict()
        }

        # Determine the correct template ID based on Custom_Form value
        if custom_form_value == "Synergetics - Quote":
            template_id = REGULAR_QUOTE_TEMPLATE_ID
            quote_template_name = "Regular Quote"
        elif custom_form_value == "Synergetics - e-Rate Quote":
            template_id = ERATE_QUOTE_TEMPLATE_ID
            quote_template_name = "Erate Quote"
        elif custom_form_value == "Synergetics Quote - Contract Renewals":
            template_id = RENEWAL_QUOTE_TEMPLATE_ID
            quote_template_name = "Renewal Quote"
        else:
            print(f"Skipping row {index}, 'Custom_Form' value '{custom_form_value}' is not recognized.")
            continue

        quote_id = post_quote_by_template(template_id, quote_data, quote_template_name)

        if quote_id:
            patch_quote(quote_id, title, document_number)

if __name__ == '__main__':
    file_path = input("Please enter the path to the CSV file: ")
    process_csv_and_post_quotes(file_path)
