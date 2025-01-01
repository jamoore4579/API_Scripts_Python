import csv

# Define the input and output file paths
input_file = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Items\\Compare.csv"
output_file = r"c:\\users\\jmoore\\documents\\connectwise\\integration\\NS_Integration\\Items\\Comparison_Results.csv"

# Open the input CSV file and read its contents
try:
    with open(input_file, 'r', encoding='utf-8-sig') as csvfile:  # Use utf-8-sig to handle BOM
        reader = csv.DictReader(csvfile)
        rows = [{key.strip(): value for key, value in row.items()} for row in reader]
except UnicodeDecodeError:
    # Retry with a fallback encoding
    with open(input_file, 'r', encoding='latin1') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [{key.strip(): value for key, value in row.items()} for row in reader]

# Prepare a list to store comparison results
comparison_results = []

# Process the rows if data exists
if rows:
    for first_row in rows:
        # Retrieve values from columns Product, Subcategory, Category in the current row
        value_product = first_row['Product']
        value_subcategory = first_row['Subcategory']
        value_category = first_row['Category']

        # Find the matching value in Column Nsproduct
        matching_row = next((row for row in rows if row['Nsproduct'] == value_product), None)

        if matching_row:
            # Retrieve corresponding values from columns Nscategory and Nssubcategory
            value_nsproduct = matching_row['Nsproduct']
            value_nscategory = matching_row['Nscategory']
            value_nssubcategory = matching_row['Nssubcategory']

            # Compare values and prepare the output
            comparison_results.append({
                'Product': value_product,
                'Nsproduct': value_nsproduct,
                'Product == Nsproduct': value_product == value_nsproduct,
                'Category': value_category,
                'Nscategory': value_nscategory,
                'Category == Nscategory': value_category == value_nscategory,
                'Subcategory': value_subcategory,
                'Nssubcategory': value_nssubcategory,
                'Subcategory == Nssubcategory': value_subcategory == value_nssubcategory,
            })

# Write the comparison results to the output CSV file
if comparison_results:
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Product', 'Nsproduct', 'Product == Nsproduct', 'Category', 'Nscategory', 'Category == Nscategory', 'Subcategory', 'Nssubcategory', 'Subcategory == Nssubcategory']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header and data rows
        writer.writeheader()
        writer.writerows(comparison_results)

print(f"Comparison results have been written to {output_file}")
