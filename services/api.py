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


def get_data_from_api(item: str):
    url = f"{API_BASE_URL}/{item}"
    headers = {
        API_HEADER_KEY_NAME: API_HEADER_KEY_VALUE,
        API_HEADER_HOST_NAME: API_HEADER_HOST_VALUE
    }
    response = requests.get(url, headers=headers)
    print(f"Getting data from: {url}")
    return response


def write_response_to_json(response, filename):
    with open(f"{SOURCE_DIR}/{filename}.json", "w") as f:
        json.dump(response.json(), f)


def pull_json_from_api(filename) -> None:
    try:
        response = get_data_from_api(filename)
        write_response_to_json(response, filename)
    except Exception as e:
        print(e)
        raise Exception
