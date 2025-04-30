import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access Sell API variables
SELL_URL = os.getenv("SELL_URL")
AUTH_ID = os.getenv("AUTH_ID")

# Access Manage API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

# Validate environment variables
if not all([SELL_URL, AUTH_ID, BASE_URL, AUTH_CODE, CLIENT_ID]):
    raise EnvironmentError("Missing one or more required environment variables.")

# Set up headers
sell_headers = {
    "Authorization": f"Basic {AUTH_ID}",
    "Content-Type": "application/json"
}

manage_headers = {
    "ClientId": CLIENT_ID,
    "Authorization": f"Basic {AUTH_CODE}",
    "Content-Type": "application/json"
}

# CSV output path
output_file_path = r"c:\users\jmoore\documents\connectwise\SalesOrders\OpenQuotesWithStatus.csv"

# Sell API endpoint
sell_endpoint = f"{SELL_URL}/quotes"

# Fields to retrieve from Sell
sell_fields = ["name", "quoteNumber", "quoteVersion", "id", "accountName", "quoteStatus", "orderPorterTemplate", "crmOpportunityId"]

# Sell API parameters
sell_params = {
    "fields": ",".join(sell_fields),
    "conditions": '(quoteStatus="Active")',
    "page": 1,
    "pageSize": 1000
}

# AND orderPorterTemplate!="ITROrders" AND orderPorterTemplate!="VAR_Sales" AND orderPorterTemplate!="Renewals"
# List to collect final combined data
combined_data = []

# Step 1: Pull active quotes from Sell
while True:
    response = requests.get(sell_endpoint, headers=sell_headers, params=sell_params)

    if response.status_code != 200:
        print(f"Error fetching Sell quotes: {response.status_code} - {response.text}")
        break

    quotes = response.json()

    if not quotes:
        break

    for quote in quotes:
        quote_record = {
            "Quote Name": quote.get("name"),
            "Quote Number": quote.get("quoteNumber"),
            "Quote Version": quote.get("quoteVersion"),
            "Quote ID": quote.get("id"),
            "Account Name": quote.get("accountName"),
            "Quote Status": quote.get("quoteStatus"),
            "Order Porter Template": quote.get("orderPorterTemplate"),
            "Opportunity": quote.get("crmOpportunityId")
        }

        crm_opp_id = quote.get("crmOpportunityId")

        # Step 2: If crmOpportunityId exists, fetch the opportunity's status name
        if crm_opp_id:
            manage_endpoint = f"{BASE_URL}/sales/opportunities/{crm_opp_id}"
            manage_params = {
                "fields": "status/name"  # Request only the status name
            }
            manage_response = requests.get(manage_endpoint, headers=manage_headers, params=manage_params)

            if manage_response.status_code == 200:
                opp_data = manage_response.json()

                if opp_data:
                    # Check if status exists
                    status_block = opp_data.get("status")
                    if status_block and isinstance(status_block, dict):
                        status_name = status_block.get("name")
                        quote_record["Opportunity Status Name"] = status_name
                    else:
                        print(f"⚠️ Opportunity ID {crm_opp_id} has no status assigned.")
                        quote_record["Opportunity Status Name"] = None
                else:
                    print(f"⚠️ Empty opportunity data returned for ID {crm_opp_id}.")
                    quote_record["Opportunity Status Name"] = None
            else:
                print(f"❌ Failed to fetch Opportunity ID {crm_opp_id}: {manage_response.status_code} - {manage_response.text}")
                quote_record["Opportunity Status Name"] = None
        else:
            # No crmOpportunityId
            quote_record["Opportunity Status Name"] = None

        combined_data.append(quote_record)

    # Pagination: If fewer than a full page, stop
    if len(quotes) < sell_params["pageSize"]:
        break

    sell_params["page"] += 1

# Step 3: Export combined data to CSV
if combined_data:
    df = pd.DataFrame(combined_data)
    df.to_csv(output_file_path, index=False)
    print(f"✅ Successfully exported {len(df)} records to '{output_file_path}'")
else:
    print("⚠️ No data to export.")
