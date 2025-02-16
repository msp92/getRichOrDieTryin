import logging

from requests import Response
from time import sleep
from sqlalchemy import and_, func
from typing import Union

from config.entity_names import (
    FIXTURE_PLAYER_STATS_API_ENDPOINT,
    FIXTURE_PLAYER_STATS_FILES_PREFIX,
    FIXTURE_PLAYER_STATS_DIR,
)
from config.vars import SLEEP_TIME
from models.data_warehouse.fixtures import Fixture
from models.data_warehouse.main import Season
from services.fetchers.api_fetcher import ApiFetcher
from services.db import Db

db = Db()


class PlayerStatsFetcher(ApiFetcher):
    def get_player_stats(self, **kwargs: dict[str, Union[int, str]]) -> Response | None:
        return self.fetch_data(FIXTURE_PLAYER_STATS_API_ENDPOINT, **kwargs)

    @staticmethod
    def get_list_of_fixtures_with_player_stats_by_dates(dates: list[str]) -> list[str]:
        with db.get_session() as session:
            try:
                leagues_with_player_stats = [
                    league_id
                    for (league_id,) in session.query(Season.league_id)
                    .filter(
                        Season.coverage["fixtures"]["statistics_players"].as_boolean()
                    )
                    .distinct()
                    .all()
                ]

                fixtures_to_pull = [
                    fixture_id
                    for (fixture_id,) in session.query(Fixture.fixture_id)
                    .filter(
                        func.date(Fixture.date).in_(dates),
                        Fixture.league_id.in_(leagues_with_player_stats),
                    )
                    .distinct()
                    .all()
                ]
                return fixtures_to_pull
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception

    def pull_player_stats_by_dates(self, dates_to_pull: list[str]) -> None:
        fixtures_to_pull = self.get_list_of_fixtures_with_player_stats_by_dates(
            dates_to_pull
        )
        if not fixtures_to_pull:
            logging.info("No fixtures to update.")
            return None

        for single_fixture in fixtures_to_pull:
            endpoint = f"{FIXTURE_PLAYER_STATS_API_ENDPOINT}?fixture={single_fixture}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(
                resp,
                f"{FIXTURE_PLAYER_STATS_FILES_PREFIX}{single_fixture}",
                f"{FIXTURE_PLAYER_STATS_DIR}",
            )
            sleep(SLEEP_TIME)

    def pull_player_statistics_for_leagues_and_seasons(
        self, league_ids_to_pull: list[int], season_year_to_pull: str
    ) -> None:
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name)
                    .filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"]),
                        )
                    )
                    .all()
                )
                for fixture in fixtures:
                    logging.info("Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        logging.info(
                            f"Pulling player statistics for fixture: {fixture.fixture_id} "
                            f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                        )
                        statistics_fixtures_data = self.fetch_data(
                            f"{FIXTURE_PLAYER_STATS_API_ENDPOINT}?fixture={fixture.fixture_id}"
                        )
                    except Exception as e:
                        logging.error(e)
                        continue
                    self.write_response_to_json(
                        statistics_fixtures_data,
                        f"{FIXTURE_PLAYER_STATS_FILES_PREFIX}{fixture.fixture_id}",
                        FIXTURE_PLAYER_STATS_DIR,
                    )
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception
