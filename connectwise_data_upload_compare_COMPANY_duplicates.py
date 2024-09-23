import os
import pandas as pd

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
    """Process company data and find matching address_number and parsed_zip for each company."""
    
    # Check if 'address_number' and 'parsed_zip' exist in the dataframe
    if 'address_number' not in dataframe.columns or 'parsed_zip' not in dataframe.columns:
        raise ValueError("'address_number' or 'parsed_zip' column not found in the dataset.")
    
    # Create a column to store the list of matched IDs for each row
    dataframe['matching_ids'] = None
    
    # Iterate over each row and compare its 'address_number' and 'parsed_zip' with all other rows
    for idx, company in dataframe.iterrows():
        # Get the address_number and parsed_zip for the current company
        address_number = company['address_number']
        parsed_zip = company['parsed_zip']
        current_id = company['id']
        
        # Find other companies with the same address_number and parsed_zip
        matching_companies = dataframe[(dataframe['address_number'] == address_number) & 
                                       (dataframe['parsed_zip'] == parsed_zip) & 
                                       (dataframe['id'] != current_id)]
        
        if not matching_companies.empty:
            # Store the matching IDs as a comma-separated string with spaces between values
            matching_ids = ', '.join(matching_companies['id'].astype(str).tolist())
            dataframe.at[idx, 'matching_ids'] = matching_ids
    
    # Order columns as specified, including the matching_ids column
    column_order = ["id", "identifier", "name", "address", "address_number", "parsed_zip", "status_name", "deletedFlag", "matching_ids"]
    dataframe = dataframe.reindex(columns=column_order)
    
    return dataframe

def upload_and_process_csv(input_file_path, output_file_path):
    """Upload a CSV file, process the data, and save the result to another CSV file."""
    try:
        # Load the company data from CSV file
        companies_df = pd.read_csv(input_file_path)
        print(f"Loaded data from '{input_file_path}'.")

        # Process the company data to find matching address_number and parsed_zip
        processed_companies_df = process_company_data(companies_df)

        # Save the processed data to the new output CSV file
        write_companies_to_csv(processed_companies_df, output_file_path)

    except Exception as e:
        print(f"Error processing file: {e}")

# Define file paths
input_file_path = r'c:\users\jmoore\documents\connectwise\projects\company_data_update_v1.csv'
output_file_path = r'c:\users\jmoore\documents\connectwise\projects\company_data_duplicates.csv'

# Call the function to upload and process the CSV file
upload_and_process_csv(input_file_path, output_file_path)
