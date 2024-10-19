from services.api_fetcher import APIFetcher
from services.db import Db

db = Db()


class StatsFetcher(APIFetcher):
    def get_stats(self, **kwargs):
        return self.fetch_data("fixtures/statistics", **kwargs)
