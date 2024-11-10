from typing import Any

from requests import Response

from services.api_fetcher import APIFetcher


class LeagueFetcher(APIFetcher):
    def get_leagues(self, **kwargs: dict[str, Any]) -> Response | None:
        return self.fetch_data("leagues", **kwargs)
