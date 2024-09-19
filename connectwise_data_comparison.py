import pandas as pd

def upload_csv(prompt):
    """Helper function to prompt user to upload CSV file."""
    print(prompt)
    path = input("Enter the path of the file: ")
    return pd.read_csv(path)

def main():
    # Upload the first CSV and collect ID_Records
    df1 = upload_csv("Please upload the first CSV file with ID_Record:")
    id_records = df1['ID_Record'].unique()

    # Upload the second CSV
    df2 = upload_csv("Please upload the second CSV file with CW_Opportunity_ID, Record_Link, and Opportunity_ID:")
    
    # Filter the second dataframe for matching CW_Opportunity_IDs
    matched_df = df2[df2['CW_Opportunity_ID'].isin(id_records)]
    
    # Extracting specific columns
    final_df = matched_df[['CW_Opportunity_ID', 'Record_Link', 'Opportunity_ID']]
    
    # Saving the output to a CSV file
    output_path = input("Enter the path to save the output file (e.g., '/Users/yourusername/Documents/matched_records.csv'): ")
    final_df.to_csv(output_path, index=False)
    print(f"File saved successfully at {output_path}")

if __name__ == "__main__":
    main()
