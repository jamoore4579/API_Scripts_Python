import pandas as pd

# Define file paths
input_path = r"C:\\users\\jmoore\\documents\\connectwise\\company\\ContactInfo.csv"
output_path = r"C:\\users\\jmoore\\documents\\connectwise\\company\\ContactInfoDuplicate.csv"

# Read the CSV file (only needed columns)
df = pd.read_csv(input_path, usecols=['id', 'firstName', 'lastName', 'company'])

# Find duplicate rows based on firstName and lastName
duplicates = df[df.duplicated(subset=['firstName', 'lastName'], keep=False)]

# Sort the results for better readability (optional)
duplicates = duplicates.sort_values(by=['firstName', 'lastName'])

# Write duplicates to output file
duplicates.to_csv(output_path, index=False)

print(f"Duplicate records written to: {output_path}")
