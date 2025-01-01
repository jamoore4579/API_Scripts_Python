import pandas as pd

# Load the input file
input_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\Production\All_Opportunities_122624.csv"
output_file = r"c:\users\jmoore\documents\connectwise\integration\NS_Integration\Opportunity\Production\Filtered_Opportunities.csv"

# Specify the columns to retrieve
columns_to_retrieve = [
    "name", "expectedCloseDate", "stage", "notes", "type", "probability",
    "salesRep", "salesEngineer", "CW_Company", "ForecastCategory", 
    "ForecastNotes", "QuotedBy", "Amount", "BDE", "470#", "471#", 
    "frnNumber", "BEN", "InvoiceByDate", "NetsuiteID"
]

# Read the CSV file
data = pd.read_csv(input_file, usecols=columns_to_retrieve)

# Remove duplicates from NetsuiteID, keeping rows where 'type' is not null
filtered_data = data.sort_values(by='type', na_position='last').drop_duplicates(subset='NetsuiteID', keep='first')

# Save the result to a new file
filtered_data.to_csv(output_file, index=False)

print(f"Filtered data saved to {output_file}")
