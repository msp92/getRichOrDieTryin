from config.config import (
    SOURCE_DIR,
    API_BASE_URL,
    API_HEADER_KEY_NAME,
    API_HEADER_KEY_VALUE,
    API_HEADER_HOST_NAME,
    API_HEADER_HOST_VALUE,
)
import requests
import json


def check_status():
    url = f"{API_BASE_URL}/status"
    headers = {
        API_HEADER_KEY_NAME: API_HEADER_KEY_VALUE,
        API_HEADER_HOST_NAME: API_HEADER_HOST_VALUE,
    }
    response = requests.get(url, headers=headers)
    response_json = response.json()["response"]
    if response.json()["response"]:
        print(
            f"Hi {response_json['account']['firstname']}!\n"
            f"Your {response_json['subscription']['plan']} plan is active until {response_json['subscription']['end']}\n"
            f"Current usage: {response_json['requests']['current']}/{response_json['requests']['limit_day']}\n"
        )
        return True
    print("[ERROR] You have reached the request limit for today. Try again later.")
    return False


def get_data_from_api(endpoint: str):
    url = f"{API_BASE_URL}/{endpoint}"
    headers = {
        API_HEADER_KEY_NAME: API_HEADER_KEY_VALUE,
        API_HEADER_HOST_NAME: API_HEADER_HOST_VALUE,
    }
    response = requests.get(url, headers=headers)
    print(f"Getting data from: {url}")
    return response


def write_response_to_json(response, filename):
    with open(f"{SOURCE_DIR}/{filename}.json", "w") as f:
        json.dump(response.json(), f)


def pull_json_from_api(endpoint):
    response = get_data_from_api(endpoint)
    if response.status_code == 200:
        if len(response.json()["response"]) == 0:
            return None
        if response.json()["paging"]["total"] > 1:
            print(
                f"[FIXME] There are more than 1 page. Please handle this later."
            )  # TODO: handle this later
        return response
    else:
        print(f"Error fetching data from API. Status Code: {response.status_code}")
        return None
