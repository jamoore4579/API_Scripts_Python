import requests
import csv

# API URL
url = "https://opendata.usac.org/resource/hbj5-2bpj.json?application_number=241006669"

# Path to save the results CSV file
results_file_path = r'c:\users\jmoore\documents\connectwise\erate_data.csv'

# Fetch the data from the API
response = requests.get(url)
data = response.json()

# Define the CSV file headers (keys from the data)
if data:
    headers = data[0].keys()  # Extract headers from the first item

# Write data to a CSV file
with open(results_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()  # Write the header
    writer.writerows(data)  # Write the data rows

print(f"Data has been saved to {results_file_path}")
