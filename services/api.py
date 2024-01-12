from config.config import (
    SOURCE_DIR,
    API_BASE_URL,
    API_HEADER_KEY_NAME,
    API_HEADER_KEY_VALUE,
    API_HEADER_HOST_NAME,
    API_HEADER_HOST_VALUE
)
import requests
import json


def get_data_from_api(item):
    headers = {
        API_HEADER_KEY_NAME: API_HEADER_KEY_VALUE,
        API_HEADER_HOST_NAME: API_HEADER_HOST_VALUE
    }
    response = requests.get(f"{API_BASE_URL}/{item}", headers=headers)
    return response


def write_response_to_json(response, filename):
    with open(f"../{SOURCE_DIR}/{filename}.json", "w") as f:
        json.dump(response.json(), f)
