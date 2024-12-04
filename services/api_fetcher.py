import json
import logging
from typing import Any

import requests
from requests import Response

from config.api_config import ApiConfig
from config.vars import DATA_DIR, ROOT_DIR
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class APIFetcher:
    def __init__(self, config: ApiConfig) -> None:
        self.config = config

    def fetch_data(self, endpoint: str, **kwargs: dict[str, Any]) -> Response | None:
        # Check subscription status before making the request
        if not self.config.has_quota():
            raise Exception("Quota exceeded. Cannot make any more requests.")

        url = f"{self.config.get_base_url()}/{endpoint}"
        headers = self.config.get_headers()
        logging.info(f"Getting data from: {url}")
        response = requests.get(url, headers=headers, params=kwargs)
        if response.status_code == 200:
            if len(response.json()["response"]) == 0:
                logging.info("Response empty. No data have been pulled.")
                return None
            return response
        else:
            logging.error(
                f"Error fetching data from API. Status Code: {response.status_code}"
            )
            return None

    @staticmethod
    def write_response_to_json(
        response: Response | None, filename: str, subdir: str = ""
    ) -> None:
        if response:
            file_path = f"{ROOT_DIR}/{DATA_DIR}/{subdir}/{filename}.json"
            try:
                with open(file_path, "w") as file:
                    json.dump(response.json(), file)
            except (AttributeError, IOError) as e:
                logging.error(f"Writing response to '{file_path}' has failed: {e}")
                raise e
