from typing import Union

from config.entity_names import LEAGUES_API_ENDPOINT, LEAGUES_DIR
from services.api_fetcher import ApiFetcher


class LeagueFetcher(ApiFetcher):
    def fetch_all_leagues(self, **kwargs: dict[str, Union[int, str]]) -> None:
        leagues_data = self.fetch_data(LEAGUES_API_ENDPOINT, **kwargs)
        # Check if teams_data is not empty
        if leagues_data:
            self.write_response_to_json(leagues_data, "LEAGUES", LEAGUES_DIR)
