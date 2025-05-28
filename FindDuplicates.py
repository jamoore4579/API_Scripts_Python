import pandas as pd
import os

# Define file paths
file_path = r"C:\users\jmoore\documents\connectwise\company\ContactInfo051625.csv"
output_path = r"C:\users\jmoore\documents\connectwise\company\DuplicateContacts.csv"

# Check if file exists
if not os.path.isfile(file_path):
    raise FileNotFoundError(f"The file was not found: {file_path}")

# Read the CSV file
df = pd.read_csv(file_path)

# Check required columns
required_columns = {'id', 'firstName', 'lastName', 'companyId'}
if not required_columns.issubset(df.columns):
    raise ValueError(f"CSV must contain columns: {required_columns}")

# Filter out rows where companyId is null or in [2, 135]
filtered_df = df[~df['companyId'].isin([2, 3, 133, 135, 138, 283, 565, 4076, 6956]) & df['companyId'].notna()]

# Find duplicates based on firstName, lastName, and companyId (excluding the first occurrence)
duplicates = filtered_df[filtered_df.duplicated(subset=["firstName", "lastName", "companyId"], keep='first')]

# Save results
duplicates.to_csv(output_path, index=False)

print(f"Found {len(duplicates)} duplicate contact(s) after filtering. Results saved to: {output_path}")
