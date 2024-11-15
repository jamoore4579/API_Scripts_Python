import os
import pandas as pd
import requests
from dotenv import load_dotenv
import numpy as np

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
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2023.1"
}

# Path to the CSV file and the output file
csv_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\All_Items_111524.csv"
output_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\All_Items_111524_results.csv"

# Define columns to read
columns_to_read = [
    "ProductID", "subCategory", "ProductType", "Serialized", "customerDescription",
    "Manufacturer", "RenewableItem", "SpecialOrderItem", "PreferredVendor",
    "vendorAddress", "vendorCity", "vendorState", 
    "vendorZip", "vendorPhone"
]

# Conversion function for RenewableItem and SpecialOrderItem values
def yes_no_to_bool(value):
    if isinstance(value, str):
        return True if value.lower() == 'yes' else False
    return value

# Sanitize data to replace NaN, inf, -inf with None and convert numpy bools to Python bools
def sanitize_data(data_dict):
    for key, value in data_dict.items():
        if isinstance(value, dict):
            sanitize_data(value)  # Recursively sanitize nested dictionaries
        elif isinstance(value, (float, np.float64)) and (np.isnan(value) or np.isinf(value)):
            data_dict[key] = None
        elif isinstance(value, np.bool_):  # Convert numpy.bool_ to Python bool
            data_dict[key] = bool(value)
    return data_dict

# Sanitize strings to handle unsupported characters
def sanitize_string(value):
    if isinstance(value, str):
        return value.encode('utf-8', 'replace').decode('utf-8')
    return value

# Lookup functions for subcategory, type, and manufacturer
def lookup_subcategory(subcategory_name):
    url = f"{BASE_URL}/procurement/subcategories"
    params = {"conditions": f"name like '{subcategory_name}%'"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None

def lookup_type(type_name):
    url = f"{BASE_URL}/procurement/types"
    params = {"conditions": f"name like '{type_name}%'"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None

def lookup_manufacturer(manufacturer_name):
    url = f"{BASE_URL}/procurement/manufacturers"
    params = {"conditions": f"name like '{manufacturer_name}%'"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None

def lookup_or_create_vendor(row):
    preferred_vendor = row['PreferredVendor']
    if not preferred_vendor:
        return "", None, "No Preferred Vendor Specified"

    lookup_url = f"{BASE_URL}/company/companies"
    lookup_params = {"conditions": f"name like '{preferred_vendor}%'"}
    lookup_response = requests.get(lookup_url, headers=headers, params=lookup_params)

    vendor_lookup_status = f"Vendor Lookup Status: {lookup_response.status_code}"
    if lookup_response.status_code == 200:
        companies = lookup_response.json()
        if companies:
            vendor_lookup_status += f" | Vendor ID Found: {companies[0]['id']}"
            return companies[0]['id'], preferred_vendor, vendor_lookup_status
    vendor_lookup_status += " | Vendor Not Found"
    return "", preferred_vendor, vendor_lookup_status

# Prepare output CSV for writing results
with open(output_path, mode='w', newline='', encoding='utf-8') as output_file:
    fieldnames = ["ProductID", "CW_ID", "HTTP_Status", "Error_Message", "Vendor_Lookup_Status"]
    writer = pd.DataFrame(columns=fieldnames)
    writer.to_csv(output_file, mode='a', index=False, header=True, encoding='utf-8')

# Read CSV in chunks and process each row individually
chunk_iter = pd.read_csv(csv_path, usecols=columns_to_read, chunksize=1, encoding='utf-8')

for chunk in chunk_iter:
    row = chunk.iloc[0].fillna("")  # Replace NaN with empty strings for each row
    row['RenewableItem'] = yes_no_to_bool(row['RenewableItem'])
    row['SpecialOrderItem'] = yes_no_to_bool(row['SpecialOrderItem'])

    # Limit the 'customerDescription' to 60 characters or use 'ProductID' if blank
    truncated_description = str(row["customerDescription"])[:60] if row["customerDescription"] else str(row["ProductID"])

    # Perform lookups and add product to catalog
    subcategory_id = lookup_subcategory(row['subCategory'])
    type_id = lookup_type(row['ProductType'])
    manufacturer_id = lookup_manufacturer(row['Manufacturer'])
    vendor_id, vendor_name, vendor_lookup_status = lookup_or_create_vendor(row)

    # Prepare catalog entry data and sanitize it, setting productClass to "Inventory" by default
    catalog_data = {
        "identifier": str(row["ProductID"]),
        "description": truncated_description,
        "customerDescription": truncated_description,
        "subcategory": {"id": subcategory_id},
        "type": {"id": type_id},
        "manufacturer": {"id": manufacturer_id},
        "productClass": "Inventory",  # Default value for productClass
        "serializedFlag": row["Serialized"],
        "taxableFlag": True,
        "customFields": [
            {"id": 80, "caption": "Renewable", "value": row["RenewableItem"]},
            {"id": 81, "caption": "Special Order", "value": row["SpecialOrderItem"]}
        ]
    }
    catalog_data = sanitize_data(catalog_data)

    # Post the product to the catalog
    catalog_url = f"{BASE_URL}/procurement/catalog"
    catalog_response = requests.post(catalog_url, headers=headers, json=catalog_data)

    # Check if the request was successful and handle errors
    if catalog_response.status_code == 201:
        cw_id = catalog_response.json().get("id", "")
        http_status = catalog_response.status_code
        error_message = ""

        # Attempt to PATCH request to update vendor information
        if vendor_id:
            patch_url = f"{catalog_url}/{cw_id}"  # Confirmed endpoint for PATCH request
            patch_data = [
                {
                    "op": "replace",
                    "path": "/vendor",
                    "value": {"id": vendor_id}
                }
            ]

            # Send PATCH request to update the vendor information
            patch_response = requests.patch(patch_url, headers=headers, json=patch_data)
            
            # Check PATCH response status
            if patch_response.status_code == 200:
                print(f"Vendor successfully patched for Product ID {row['ProductID']} with Vendor ID {vendor_id} ({vendor_name}).")
            else:
                # Log error details if PATCH failed, including vendor details
                error_message += (f" | Vendor patch error: {patch_response.status_code} - {patch_response.text} "
                                  f"for Vendor ID {vendor_id} ({vendor_name})")
                print(f"Failed to patch vendor for Product ID {row['ProductID']}: {patch_response.status_code} - "
                      f"{patch_response.text} for Vendor ID {vendor_id} ({vendor_name})")

    else:
        cw_id = ""
        http_status = catalog_response.status_code
        error_message = catalog_response.text

    # Append result to output CSV with vendor lookup status
    with open(output_path, mode='a', newline='', encoding='utf-8') as output_file:
        writer = pd.DataFrame({
            "ProductID": [row["ProductID"]],
            "CW_ID": [cw_id],
            "HTTP_Status": [http_status],
            "Error_Message": [sanitize_string(error_message)],
            "Vendor_Lookup_Status": [sanitize_string(vendor_lookup_status)]
        })
        writer.to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8')

    # Print status for each product processed
    print(f"Product {row['ProductID']} processed: CW_ID={cw_id}, HTTP_Status={http_status}, "
          f"Error_Message={sanitize_string(error_message)}, Vendor_Lookup_Status={sanitize_string(vendor_lookup_status)}")
