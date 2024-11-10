from typing import Any

from requests import Response

from services.api_fetcher import APIFetcher
from services.db import Db

db = Db()


class StatsFetcher(APIFetcher):
    def get_stats(self, **kwargs: dict[str, Any]) -> Response | None:
        return self.fetch_data("fixtures/statistics", **kwargs)
