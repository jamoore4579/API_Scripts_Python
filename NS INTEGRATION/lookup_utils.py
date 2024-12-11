import requests

def lookup_subcategory(category_name, subcategory_name, base_url, headers):
    """
    Lookup subcategory ID by category name and subcategory name.

    Args:
        category_name (str): The name of the category.
        subcategory_name (str): The name of the subcategory.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        str or None: The subcategory ID if found, else None.
    """
    if not category_name or not subcategory_name:
        return None

    url = f"{base_url}/procurement/subcategories"
    params = {"conditions": f"category/name like '{category_name}%' and name like '{subcategory_name}%'"}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None


def lookup_type(type_name, base_url, headers):
    """
    Lookup type ID by name.

    Args:
        type_name (str): The name of the type.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        str or None: The type ID if found, else None.
    """
    if not type_name:
        return None

    url = f"{base_url}/procurement/types"
    params = {"conditions": f"name like '{type_name}%'"}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None


def lookup_manufacturer(manufacturer_name, base_url, headers):
    """
    Lookup manufacturer ID by name.

    Args:
        manufacturer_name (str): The name of the manufacturer.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        str or None: The manufacturer ID if found, else None.
    """
    if not manufacturer_name:
        return None

    url = f"{base_url}/procurement/manufacturers"
    params = {"conditions": f"name like '{manufacturer_name}%'"}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        result = response.json()
        return result[0]['id'] if result else None
    return None


def lookup_uom(uom_name, base_url, headers):
    """
    Lookup unit of measure ID by name.

    Args:
        uom_name (str): The name of the unit of measure.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        str or None: The unit of measure ID if found, else None.
    """
    if not uom_name:
        return None

    url = f"{base_url}/procurement/unitOfMeasures"
    params = {"conditions": f"name like '{uom_name}%'"}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        uoms = response.json()
        return uoms[0]['id'] if uoms else None
    return None


def lookup_vendor(preferred_vendor, base_url, headers):
    """
    Lookup vendor ID by name.

    Args:
        preferred_vendor (str): The name of the preferred vendor.
        base_url (str): The base URL of the API.
        headers (dict): Headers for the API request.

    Returns:
        str or None: The vendor ID if found, else None.
    """
    if not preferred_vendor:
        return None

    lookup_url = f"{base_url}/company/companies"
    lookup_params = {"conditions": f"name like '{preferred_vendor}%'"}
    response = requests.get(lookup_url, headers=headers, params=lookup_params)

    if response.status_code == 200:
        companies = response.json()
        return companies[0]['id'] if companies else None
    return None
