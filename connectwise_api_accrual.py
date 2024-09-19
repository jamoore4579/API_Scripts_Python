## Upload Monthly Time Accruals

import os, requests
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# accessing API variables
BASE_URL = os.getenv("BASE_URL")
AUTH_CODE = os.getenv("AUTH_CODE")
CLIENT_ID = os.getenv("CLIENT_ID")

headers = {
    "ClientId": CLIENT_ID,
    "Authorization": "Basic " + AUTH_CODE
}

try:
    response = requests.get(url=BASE_URL + "/system/members/368/accruals", headers=headers)
    response.raise_for_status()

    print("Response Code: ", response.status_code)
    print("Response Content: ", response.text)

except requests.exceptions.HTTPError as errh:
    print(f"HTTP Error: {errh}")
except requests.exceptions.RequestExceptions as err:
    print(f"Request Error: {err}")