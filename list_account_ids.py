import json
import requests
from pprint import pprint

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

API_BASE_URL = "https://demo-api-capital.backend-capital.com"
API_KEY = config["capital_api_key"]
IDENTIFIER = config["capital_login"]
PASSWORD = config["capital_pw"]

headers = {
    "X-CAP-API-KEY": API_KEY,
    "Content-Type": "application/json",
}

payload = {
    "identifier": IDENTIFIER,
    "password": PASSWORD,
    "encryptedPassword": False,
}

r = requests.post(f"{API_BASE_URL}/api/v1/session", headers=headers, json=payload)
r.raise_for_status()

headers["CST"] = r.headers["CST"]
headers["X-SECURITY-TOKEN"] = r.headers["X-SECURITY-TOKEN"]

accounts = requests.get(f"{API_BASE_URL}/api/v1/accounts", headers=headers)
accounts.raise_for_status()

pprint(accounts.json())
