import json
import logging
import requests
from requests import Response

from config.api_config import ApiConfig
from config.vars import SOURCE_DIR


class APIFetcher:
    def __init__(self, config: ApiConfig) -> None:
        self.config = config

    def fetch_data(self, endpoint: str, **kwargs) -> Response | None:
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
            if response.json()["paging"]["total"] > 1:
                # FIXME: handle more than 1 page
                logging.warning(
                    "[FIXME] There are more than 1 page. Please handle this asap."
                )
                return None
            return response
        else:
            logging.error(
                f"Error fetching data from API. Status Code: {response.status_code}"
            )
            return None

    @staticmethod
    def write_response_to_json(
        response: Response | None, filename: str, subdir=""
    ) -> None:
        if response:
            # file_path = os.path.join(SOURCE_DIR, subdir, f"{filename}.json")
            file_path = f"../{SOURCE_DIR}/{subdir}/{filename}.json"
            try:
                with open(file_path, "w") as file:
                    json.dump(response.json(), file)
            except (AttributeError, IOError) as e:
                logging.error(f"Writing response to '{file_path}' has failed: {e}")
                raise e
