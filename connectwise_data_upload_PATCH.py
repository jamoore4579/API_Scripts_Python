import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

def check_opportunity_status(opportunity_id):
    try:
        response = requests.get(url=BASE_URL + f"/sales/opportunities/{opportunity_id}", headers=headers)
        response.raise_for_status()
        opportunity_data = response.json()
        status = opportunity_data.get("status", {}).get("name")
        return status, opportunity_data
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        return None, None
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return None, None

def upload_cw_data():
    # Ask the user for the CSV file path
    file_name = input("Enter the relative path to the CSV file: ")

    # Get the current directory
    base_path = os.getcwd()

    # Construct the full file path
    csv_file_path = os.path.abspath(os.path.join(base_path, file_name))

    # Load CSV data
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File '{csv_file_path}' not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: File '{csv_file_path}' is empty.")
        return
    except pd.errors.ParserError:
        print(f"Error: Unable to parse file '{csv_file_path}'. Make sure it is a valid CSV file.")
        return

    # Extract relevant data from the DataFrame (adjust column names accordingly)
    CW_Opportunity_ID = df['CW_Opportunity_ID'].tolist()
    SF_Opportunity_Link = df['SF_Opportunity_Link'].tolist()
    SF_Opportunity_ID = df['SF_Opportunity_ID'].tolist()
    

    # Create a list to store results
    results = []

    # Iterate through data and make API requests
    for i in range(len(CW_Opportunity_ID)):
        status, opportunity_data = check_opportunity_status(CW_Opportunity_ID[i])
        
        if status == "Open":
            opportunity_data = [
                {
                    "op": "replace",
                    "path": "/customFields",
                    "value": [
                        {
                            "id": 21,
                            "value": SF_Opportunity_Link[i]
                        },
                         {
                            "id": 27,
                            "value": SF_Opportunity_ID[i]
                        },
                    ]
                }
            ]
            try:
                response = requests.patch(url=BASE_URL + f"/sales/opportunities/{CW_Opportunity_ID[i]}", headers=headers, json=opportunity_data)
                response.raise_for_status()

                print(f"Updated Opportunity ID {CW_Opportunity_ID[i]}")
                filtered_response = [
                    {
                        "id": 21, "caption": "SF Opportunity Link Update", "value": SF_Opportunity_Link[i]
                    },
                    {
                        "id": 27, "caption": "SF Opportunity ID update", "value": SF_Opportunity_ID[i]
                    }
                    
                ]
                print("Response Content:", filtered_response)
                print("Status of Opportunity:", status)  # Print status of the opportunity

                # Append result to the list
                results.append({"Opportunity ID": CW_Opportunity_ID[i], "Status": "Updated", "Response Code": response.status_code, "Response Content": filtered_response})

            except requests.exceptions.HTTPError as errh:
                print(f"HTTP Error: {errh}")
                # Append error result to the list
                results.append({"Opportunity ID": CW_Opportunity_ID[i], "Status": "Error", "Error": str(errh)})
            except requests.exceptions.RequestException as err:
                print(f"Request Error: {err}")
                # Append error result to the list
                results.append({"Opportunity ID": CW_Opportunity_ID[i], "Status": "Error", "Error": str(err)})

    # Write results to CSV file
    results_df = pd.DataFrame(results)
    results_file_path = r'c:\users\jmoore\documents\connectwise\upload_results.csv'
    results_df.to_csv(results_file_path, index=False)
    print(f"Results written to '{results_file_path}'.")

# Call the function to load and upload accruals
upload_cw_data()
