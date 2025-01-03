import os
import pandas as pd
import requests
from dotenv import load_dotenv
import numpy as np

# Load environment variables
load_dotenv()

# Accessing API variables from environment file
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Set up headers for the API request
headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE,
    "Content-Type": "application/json",
    "Accept": "application/vnd.connectwise.com+json; version=2023.1"
}

# Path to the CSV file and the output file
csv_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\All_Items_120224.csv"
output_path = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Items\Production\All_Items_120224_results.csv"

# Define columns to read
columns_to_read = [
    "ProductID", "subCategory", "uom", "Class", "customerDescription", "notes", "Serialized", 
    "Manufacturer", "ProductType", "RenewableItem", "Price", "PreferredVendor"
]

# Conversion function for RenewableItem values
def yes_no_to_bool(value):
    if isinstance(value, str):
        return True if value.lower() == 'yes' else False
    return value

# Sanitize data to replace NaN, inf, -inf with None and convert numpy types to native Python types
def sanitize_data(data_dict):
    for key, value in data_dict.items():
        if isinstance(value, dict):
            sanitize_data(value)  # Recursively sanitize nested dictionaries
        elif isinstance(value, (float, np.float64)) and (np.isnan(value) or np.isinf(value)):
            data_dict[key] = None
        elif isinstance(value, (np.bool_, np.int64, np.float64)):  # Convert numpy types to Python types
            data_dict[key] = value.item()  # Convert numpy type to its Python equivalent
    return data_dict

# Sanitize strings to handle unsupported characters
def sanitize_string(value):
    if isinstance(value, str):
        return value.encode('utf-8', 'replace').decode('utf-8')
    return value

# Lookup functions for subcategory, type, manufacturer, and unit of measure
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

def lookup_uom(uom_name):
    if not uom_name:
        return None, "No UOM Specified"
    
    url = f"{BASE_URL}/procurement/unitOfMeasures"
    params = {"conditions": f"name like '{uom_name}%'"}
    response = requests.get(url, headers=headers, params=params)

    uom_lookup_status = f"UOM Lookup Status: {response.status_code}"
    if response.status_code == 200:
        uoms = response.json()
        if uoms:
            uom_lookup_status += f" | UOM ID Found: {uoms[0]['id']}"
            return uoms[0]['id'], uom_lookup_status
    uom_lookup_status += " | UOM Not Found"
    return None, uom_lookup_status

def lookup_vendor(preferred_vendor):
    if not preferred_vendor:
        return None, "No Preferred Vendor Specified"

    lookup_url = f"{BASE_URL}/company/companies"
    lookup_params = {"conditions": f"name like '{preferred_vendor}%'"}
    lookup_response = requests.get(lookup_url, headers=headers, params=lookup_params)

    vendor_lookup_status = f"Vendor Lookup Status: {lookup_response.status_code}"
    if lookup_response.status_code == 200:
        companies = lookup_response.json()
        if companies:
            vendor_lookup_status += f" | Vendor ID Found: {companies[0]['id']}"
            return companies[0]['id'], vendor_lookup_status
    vendor_lookup_status += " | Vendor Not Found"
    return None, vendor_lookup_status

# Prepare output CSV for writing results
with open(output_path, mode='w', newline='', encoding='utf-8') as output_file:
    fieldnames = ["ProductID", "CW_ID", "HTTP_Status", "Error_Message", "Vendor_Lookup_Status", "UOM_Lookup_Status"]
    writer = pd.DataFrame(columns=fieldnames)
    writer.to_csv(output_file, mode='a', index=False, header=True, encoding='utf-8')

# Read CSV in chunks and process rows
chunk_iter = pd.read_csv(csv_path, usecols=columns_to_read, chunksize=1, encoding='utf-8')

# Process first 5 records for initial check
for idx, chunk in enumerate(chunk_iter):
    row = chunk.iloc[0].fillna("")  # Replace NaN with empty strings for each row
    row['RenewableItem'] = yes_no_to_bool(row['RenewableItem'])

    # Retain the original value of "Class"
    product_class = row["Class"]

    # Retrieve the 'Price' and 'notes' values
    price_value = row["Price"]
    notes_value = row["notes"]

    # Limit the 'customerDescription' to 60 characters or use 'ProductID' if blank
    truncated_description = str(row["customerDescription"])[:60] if row["customerDescription"] else str(row["ProductID"])

    # Perform lookups
    subcategory_id = lookup_subcategory(row['subCategory'])
    type_id = lookup_type(row['ProductType'])
    manufacturer_id = lookup_manufacturer(row['Manufacturer'])
    vendor_id, vendor_lookup_status = lookup_vendor(row['PreferredVendor'])
    uom_id, uom_lookup_status = lookup_uom(row['uom'])

    # Prepare catalog entry data
    catalog_data = {
        "identifier": str(row["ProductID"]),
        "description": truncated_description,
        "customerDescription": truncated_description,
        "subcategory": {"id": subcategory_id},
        "type": {"id": type_id},
        "manufacturer": {"id": manufacturer_id},
        "vendor": {"id": vendor_id},
        "unitOfMeasure": {"id": uom_id},
        "productClass": product_class,
        "serializedFlag": bool(row["Serialized"]),
        "taxableFlag": True,
        "cost": float(price_value),
        "notes": notes_value,
        "customFields": [
            {"id": 80, "caption": "Renewable", "value": row["RenewableItem"]},
        ]
    }
    catalog_data = sanitize_data(catalog_data)

    # Post the product to the catalog
    catalog_url = f"{BASE_URL}/procurement/catalog"
    catalog_response = requests.post(catalog_url, headers=headers, json=catalog_data)

    # Handle response
    if catalog_response.status_code == 201:
        cw_id = catalog_response.json().get("id", "")
        http_status = catalog_response.status_code
        error_message = ""
    else:
        cw_id = ""
        http_status = catalog_response.status_code
        error_message = catalog_response.text

    # Append result to output CSV
    with open(output_path, mode='a', newline='', encoding='utf-8') as output_file:
        writer = pd.DataFrame({
            "ProductID": [row["ProductID"]],
            "CW_ID": [cw_id],
            "HTTP_Status": [http_status],
            "Error_Message": [sanitize_string(error_message)],
            "Vendor_Lookup_Status": [sanitize_string(vendor_lookup_status)],
            "UOM_Lookup_Status": [sanitize_string(uom_lookup_status)]
        })
        writer.to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8')

    print(f"Processed Product {row['ProductID']}: HTTP_Status={http_status}, Error={error_message}")

    # Pause after processing the first 5 records
    if idx == 4:
        proceed = input("The first 5 records have been processed. Do you want to continue? (yes/no): ").strip().lower()
        if proceed != "yes":
            print("Exiting the script as requested.")
            break
