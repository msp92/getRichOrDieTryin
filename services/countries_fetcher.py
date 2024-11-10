from typing import Any

from requests import Response

from services.api_fetcher import APIFetcher


class CountryFetcher(APIFetcher):
    def get_countries(self, **kwargs: dict[str, Any]) -> Response | None:
        return self.fetch_data("countries", **kwargs)
