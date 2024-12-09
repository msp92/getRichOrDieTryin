import logging
from time import sleep
from typing import Union

from requests import Response
from sqlalchemy import func

from config.entity_names import (
    FIXTURE_STATS_API_ENDPOINT,
    FIXTURE_STATS_FILES_PREFIX,
    FIXTURE_STATS_DIR,
)
from config.vars import SLEEP_TIME
from models.data.fixtures import Fixture
from models.data.main import Season
from services.api_fetcher import ApiFetcher
from services.db import Db

db = Db()


class StatsFetcher(ApiFetcher):
    def get_stats(self, **kwargs: dict[str, Union[int, str]]) -> Response | None:
        return self.fetch_data(FIXTURE_STATS_API_ENDPOINT, **kwargs)

    @staticmethod
    def get_list_of_fixtures_with_stats_by_dates(dates: list[str]) -> list[str]:
        with db.get_session() as session:
            try:
                leagues_with_fixture_stats = [
                    league_id
                    for (league_id,) in session.query(Season.league_id)
                    .filter(
                        Season.coverage["fixtures"]["statistics_fixtures"].as_boolean()
                    )
                    .distinct()
                    .all()
                ]

                fixtures_to_pull = [
                    fixture_id
                    for (fixture_id,) in session.query(Fixture.fixture_id)
                    .filter(
                        func.date(Fixture.date).in_(dates),
                        Fixture.league_id.in_(leagues_with_fixture_stats),
                    )
                    .distinct()
                    .all()
                ]
                return fixtures_to_pull
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception

    def pull_stats_by_dates(self, dates_to_pull: list[str]) -> None:
        fixtures_to_pull = self.get_list_of_fixtures_with_stats_by_dates(dates_to_pull)
        if not fixtures_to_pull:
            logging.info("No fixtures to update.")
            return None

        for single_fixture in fixtures_to_pull:
            endpoint = f"{FIXTURE_STATS_API_ENDPOINT}?fixture={single_fixture}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(
                resp,
                f"{FIXTURE_STATS_FILES_PREFIX}{single_fixture}",
                f"{FIXTURE_STATS_DIR}",
            )
            sleep(SLEEP_TIME)
