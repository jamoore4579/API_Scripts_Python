import pandas as pd
import os
from datetime import datetime, timedelta

# Load the CSV file
input_file = r'C:\Users\jmoore\Documents\Netsuite\Proliant_Time.csv'
data = pd.read_csv(input_file)

# Filter relevant columns
data = data[['ID', 'FirstName', 'LastName', 'PayrollItem', 'Date', 'Hours']]
data['Date'] = pd.to_datetime(data['Date'])

# Define the date range
date_start = datetime(2024, 12, 15)
date_end = datetime(2024, 12, 28)
date_range = [date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1)]

# Initialize the output dataframe
output_columns = [
    'ID', 'First Name', 'Last Name', 'Holiday Pay', 'Hourly Pay', 'Overtime Pay',
    'Paid Time Off', 'Paid Time Off - Unscheduled', 'Salary Pay', 'Salary Projects', 'Total Hours'
] + [date.strftime('%m/%d/%y') for date in date_range]
output_data = pd.DataFrame(columns=output_columns)

# Group data by ID, FirstName, LastName
rows = []
for (id, first_name, last_name), group in data.groupby(['ID', 'FirstName', 'LastName']):
    # Initialize row data
    row = {
        'ID': id,
        'First Name': first_name,
        'Last Name': last_name,
        'Holiday Pay': 0,
        'Hourly Pay': 0,
        'Overtime Pay': 0,
        'Paid Time Off': 0,
        'Paid Time Off - Unscheduled': 0,
        'Salary Pay': 0,
        'Salary Projects': 0,
        'Total Hours': 0  # Initialize Total Hours
    }
    # Add columns for each date
    for date in date_range:
        row[date.strftime('%m/%d/%y')] = 0

    # Aggregate data
    for _, record in group.iterrows():
        date_str = record['Date'].strftime('%m/%d/%y')
        if date_str in row:
            row[date_str] += record['Hours']

        if record['PayrollItem'] == 'Holiday Pay':
            row['Holiday Pay'] += record['Hours']
        elif record['PayrollItem'] == 'Hourly Pay':
            row['Hourly Pay'] += record['Hours']
        elif record['PayrollItem'] == 'Overtime Hourly':
            row['Overtime Pay'] += record['Hours']
        elif record['PayrollItem'] == 'Paid Time Off':
            row['Paid Time Off'] += record['Hours']
        elif record['PayrollItem'] == 'Paid Time Off - Unscheduled':
            row['Paid Time Off - Unscheduled'] += record['Hours']
        elif record['PayrollItem'] == 'Salary Pay':
            row['Salary Pay'] += record['Hours']
        elif record['PayrollItem'] == 'Salary Projects':
            row['Salary Projects'] += record['Hours']

    # Calculate Total Hours
    row['Total Hours'] = (
        row['Holiday Pay'] + row['Hourly Pay'] + row['Overtime Pay'] +
        row['Paid Time Off'] + row['Paid Time Off - Unscheduled'] +
        row['Salary Pay'] + row['Salary Projects']
    )

    # Append row to the list
    rows.append(row)

# Convert rows to DataFrame
output_data = pd.DataFrame(rows, columns=output_columns)

# Save the output to a new CSV file
output_file = os.path.join(os.path.dirname(input_file), 'Processed_Proliant_Time.csv')
output_data.to_csv(output_file, index=False)

print(f"Output file created at: {output_file}")
