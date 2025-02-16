from typing import Union

from config.entity_names import COUNTRIES_API_ENDPOINT, COUNTRIES_DIR
from services.fetchers.api_fetcher import ApiFetcher


class CountryFetcher(ApiFetcher):
    def fetch_all_countries(self, **kwargs: dict[str, Union[int, str]]) -> None:
        countries_data = self.fetch_data(COUNTRIES_API_ENDPOINT, **kwargs)
        # Check if teams_data is not empty
        if countries_data:
            self.write_response_to_json(countries_data, "COUNTRIES", COUNTRIES_DIR)
