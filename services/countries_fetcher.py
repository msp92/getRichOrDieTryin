from typing import Any

from requests import Response

from config.entity_names import COUNTRIES_API_ENDPOINT, COUNTRIES_DIR
from services.api_fetcher import APIFetcher


class CountryFetcher(APIFetcher):
    def fetch_all_countries(self, **kwargs: dict[str, Any]) -> Response | None:
        countries_data = self.fetch_data(COUNTRIES_API_ENDPOINT, **kwargs)
        # Check if teams_data is not empty
        if countries_data:
            self.write_response_to_json(countries_data, "COUNTRIES", COUNTRIES_DIR)
