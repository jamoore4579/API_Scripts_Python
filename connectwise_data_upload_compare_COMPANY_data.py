import os
import pandas as pd
import re  # Import regex for parsing address and zip codes

def parse_address_number(address):
    """Parse the first segment of the address as the address number."""
    if address and isinstance(address, str):
        match = re.match(r'^\d+', address)  # Match the first number at the start of the string
        if match:
            return match.group(0)  # Return the address number
    return 'No Address Number'

def parse_zip_code(zip_code):
    """Parse the zip code to extract a valid 5-digit zip code if available."""
    if zip_code and isinstance(zip_code, str):
        match = re.match(r'^\d{5}', zip_code)  # Match a 5-digit zip code at the start of the string
        if match:
            return match.group(0)
    return 'Invalid Zip'

def write_companies_to_csv(dataframe, file_path):
    try:
        # Check if the file already exists
        if os.path.isfile(file_path):
            # Append to the CSV file without writing the header
            dataframe.to_csv(file_path, mode='a', header=False, index=False)
        else:
            # Write to a new CSV file with the header
            dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Company data written to '{file_path}'.")
    except Exception as e:
        print(f"Error writing to file: {e}")

def process_company_data(dataframe):
    """Process company data by parsing address and zip code."""
    dataframe['address_number'] = dataframe['address'].apply(parse_address_number)
    dataframe['parsed_zip'] = dataframe['zip'].apply(parse_zip_code)

    # Order columns as specified, including parsed address and zip
    column_order = ["id", "identifier", "name", "address", "address_number", "parsed_zip", "status_name", "deletedFlag"]
    dataframe = dataframe.reindex(columns=column_order)
    
    return dataframe

def upload_and_process_csv(input_file_path, output_file_path):
    """Upload a CSV file, process the data, and save the result to another CSV file."""
    try:
        # Load the company data from CSV file
        companies_df = pd.read_csv(input_file_path)
        print(f"Loaded data from '{input_file_path}'.")

        # Process the company data by parsing address and zip
        processed_companies_df = process_company_data(companies_df)

        # Save the processed data to the new output CSV file
        write_companies_to_csv(processed_companies_df, output_file_path)

    except Exception as e:
        print(f"Error processing file: {e}")

# Define file paths
input_file_path = r'c:\users\jmoore\documents\connectwise\projects\company_data.csv'
output_file_path = r'c:\users\jmoore\documents\connectwise\projects\company_data_update.csv'

# Call the function to upload and process the CSV file
upload_and_process_csv(input_file_path, output_file_path)
