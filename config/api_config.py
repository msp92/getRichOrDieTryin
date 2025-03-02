import logging
import os
import requests
from dotenv import load_dotenv

env_file = ".env.rds" if os.getenv("DOCKER_ENV") else ".env.local"
load_dotenv(env_file)


class ApiConfig:
    def __init__(self) -> None:
        self.base_url = os.getenv("API_FOOTBALL_BASE_URL", "")
        self.key_name = os.getenv("API_FOOTBALL_HEADER_KEY_NAME", "")
        self.key_value = os.getenv("API_FOOTBALL_HEADER_KEY_VALUE", "")
        self.host_name = os.getenv("API_FOOTBALL_HEADER_HOST_NAME", "")
        self.host_value = os.getenv("API_FOOTBALL_HEADER_HOST_VALUE", "")
        self.subscription_status = None  # Cache the subscription status

    def get_headers(self) -> dict[str, str]:
        return {
            self.key_name: self.key_value,
            self.host_name: self.host_value,
        }

    def get_base_url(self) -> str | None:
        return self.base_url

    def check_subscription_status(self) -> int | None:
        """
        Checks the current subscription status or API usage limits.
        """
        if self.subscription_status is None:
            url = f"{self.get_base_url()}/status"
            headers = self.get_headers()
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Response: {response.status_code}")

            if response.json()["response"]:
                api_requests = response.json()["response"]["requests"]
                remaining_quota = api_requests["limit_day"] - api_requests["current"]
                logging.info(
                    f"\n* * * * * * * * * * "
                    f"Hi {response.json()['response']['account']['firstname']}! * * * * * * * * * * "
                    f"\nYour {response.json()['response']['subscription']['plan']} plan is active "
                    f"until {response.json()['response']['subscription']['end']}\n"
                    f"* * * * * * * * Current usage: {api_requests['current']}/{api_requests['limit_day']} * * * * * * * * "
                )
                self.subscription_status = remaining_quota  # Cache the result
            else:
                logging.error(response.json()["errors"]["requests"])
        return self.subscription_status

    def has_quota(self) -> bool:
        """
        Check if the user has enough quota left to make requests.
        """
        status = self.check_subscription_status()
        if status and status > 0:
            return True
        return False
