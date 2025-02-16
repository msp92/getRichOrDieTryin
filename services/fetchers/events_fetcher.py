import logging

from requests import Response
from sqlalchemy import and_, func
from time import sleep
from typing import Union

from config.entity_names import (
    FIXTURE_EVENTS_API_ENDPOINT,
    FIXTURE_EVENTS_FILES_PREFIX,
    FIXTURE_EVENTS_DIR,
)
from config.vars import SLEEP_TIME
from models.data_warehouse.fixtures import Fixture
from models.data_warehouse.main import Season
from services.fetchers.api_fetcher import ApiFetcher
from services.db import Db

db = Db()


class EventsFetcher(ApiFetcher):
    def get_events(self, **kwargs: dict[str, Union[int, str]]) -> Response | None:
        return self.fetch_data(FIXTURE_EVENTS_API_ENDPOINT, **kwargs)

    @staticmethod
    def get_list_of_fixtures_with_events_by_dates(dates: list[str]) -> list[str]:
        with db.get_session() as session:
            try:
                leagues_with_events = [
                    league_id
                    for (league_id,) in session.query(Season.league_id)
                    .filter(Season.coverage["fixtures"]["events"].as_boolean())
                    .distinct()
                    .all()
                ]

                fixtures_to_pull = [
                    fixture_id
                    for (fixture_id,) in session.query(Fixture.fixture_id)
                    .filter(
                        func.date(Fixture.date).in_(dates),
                        Fixture.league_id.in_(leagues_with_events),
                    )
                    .distinct()
                    .all()
                ]
                return fixtures_to_pull
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception

    def pull_events_by_dates(self, dates_to_pull: list[str]) -> None:
        fixtures_to_pull = self.get_list_of_fixtures_with_events_by_dates(dates_to_pull)
        if not fixtures_to_pull:
            logging.info("No fixtures to update.")
            return None

        for single_fixture in fixtures_to_pull:
            endpoint = f"{FIXTURE_EVENTS_API_ENDPOINT}?fixture={single_fixture}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(
                resp,
                f"{FIXTURE_EVENTS_FILES_PREFIX}{single_fixture}",
                f"{FIXTURE_EVENTS_DIR}",
            )
            sleep(SLEEP_TIME)

    def pull_events_for_leagues_and_seasons(
        self, league_ids_to_pull: list[int], season_year_to_pull: str
    ) -> None:
        finished_statuses = ["FT", "AET", "PEN", "WO"]
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name)
                    .filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(finished_statuses),
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
                        events_fixtures_data = self.fetch_data(
                            f"{FIXTURE_EVENTS_API_ENDPOINT}?fixture={fixture.fixture_id}"
                        )
                        self.write_response_to_json(
                            events_fixtures_data,
                            f"{FIXTURE_EVENTS_FILES_PREFIX}{fixture.fixture_id}",
                            FIXTURE_EVENTS_DIR,
                        )
                    except Exception as e:
                        logging.error(e)
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception
