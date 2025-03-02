# api_fetcher.py
import json
import logging
import os
import requests
from typing import Optional
from requests import Response

from config.api_config import ApiConfig
from config.vars import DATA_DIR, ROOT_DIR

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ApiFetcher:
    """
    Base class for handling HTTP requests to the external API.
    Provides:
      - Quota check
      - Basic GET request logic
      - JSON writing to disk
    """

    def __init__(self) -> None:
        self.config = ApiConfig()

    def fetch_data(self, endpoint: str, **params) -> Optional[Response]:
        """
        Executes a GET request to the specified endpoint with given params.
        Checks API quota before requesting.
        Returns a Response object or None if the request or data is invalid.
        """
        # 1. Check subscription status before making the request
        if not self.config.has_quota():
            raise Exception("Quota exceeded. Cannot make any more requests.")

        url = f"{self.config.get_base_url()}/{endpoint}"
        headers = self.config.get_headers()

        logging.info(f"Fetching data from: {url}, params={params}")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            if not response.json().get("response"):
                logging.info("Response is empty. No data returned.")
                return None
            return response
        else:
            logging.error(f"API returned status code: {response.status_code}")
            return None

    @staticmethod
    def write_response_to_json(
        response: Response, filename: str, subdir: str = ""
    ) -> None:
        """
        Writes the API JSON response to a local file.
        Creates directories if they do not exist.
        """
        if not response:
            return
        file_path = os.path.join(ROOT_DIR, DATA_DIR, subdir, f"{filename}.json")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(response.json(), file)
        except (AttributeError, IOError) as e:
            logging.error(f"Failed to write to '{file_path}': {e}")
            raise e
