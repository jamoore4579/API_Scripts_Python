import requests
import re

def extract_last_name(full_name):
    """
    Extract the last name from a full name string.

    Args:
        full_name (str): The full name of the sales representative.

    Returns:
        str: The last name if extracted, otherwise an empty string.
    """
    if not full_name:
        return ""
    match = re.search(r'\b(\w+)$', full_name.strip())
    return match.group(1) if match else ""

def fetch_sales_reps(last_name, base_url, headers):
    """
    Fetch sales representatives by their last name using ConnectWise API.

    Args:
        last_name (str): The last name of the sales representative.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        list: A list of matching sales representative data.
    """
    if not last_name:
        return []

    url = f"{base_url}/system/members"
    params = {"conditions": f"lastName like \"{last_name}%\""}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"No sales representatives found with last name '{last_name}'.")
                return []
            return data
        else:
            print(f"Failed to fetch sales reps. Status Code: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching sales reps: {str(e)}")
        return []

def lookup_sales_rep(sales_rep_name, base_url, headers):
    """
    Lookup sales representative ID by their name.

    Args:
        sales_rep_name (str): The full name of the sales representative.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        int: The ID of the sales representative if found, otherwise 0.
    """
    if not sales_rep_name:
        print("Sales representative name is empty.")
        return 0

    # Extract last name from the full name
    last_name = extract_last_name(sales_rep_name)

    if not last_name:
        print("Unable to extract last name from the provided name.")
        return 0

    # Fetch sales representative data from the API
    sales_rep_data = fetch_sales_reps(last_name, base_url, headers)

    # Attempt to find the first match and return the member ID
    if sales_rep_data:
        for rep in sales_rep_data:
            full_name_api = f"{rep.get('firstName', '')} {rep.get('lastName', '')}".strip()
            if sales_rep_name.lower() == full_name_api.lower():
                return rep.get('id', 0)
        print(f"No exact match found for '{sales_rep_name}'.")
    return 0

def fetch_order_status(status_value, base_url, headers):
    """
    Retrieve the 'Status' value from ConnectWise API using sales orders statuses endpoint.

    Args:
        status_value (str): The status value to look up.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        dict: The matching status data if found, otherwise None.
    """
    if not status_value:
        print("Status value is empty.")
        return None

    url = f"{base_url}/sales/orders/statuses"
    params = {"conditions": f"name like \"{status_value}%\""}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"No statuses found matching '{status_value}'.")
                return None
            return data[0]  # Assuming we return the first matching status
        else:
            print(f"Failed to fetch order statuses. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching order status: {str(e)}")
        return None
