import logging
from dataclasses import dataclass
import datetime as dt
from time import sleep
from typing import Optional, Union

from requests import Response

from config.api_config import ApiConfig
from config.entity_names import (
    FIXTURES_FILES_PREFIX,
    FIXTURES_API_ENDPOINT,
    FIXTURES_DIR,
)
from config.vars import SLEEP_TIME
from models.data_warehouse.main import League, Season
from services.fetchers.api_fetcher import ApiFetcher
from services.db import Db

db = Db()


@dataclass
class FixtureParams:
    status: Optional[str] = None
    page: Optional[int] = None
    limit: Optional[int] = None


class FixtureFetcher(ApiFetcher):
    def __init__(self, config: ApiConfig) -> None:  # noqa: F821
        # Call the parent class (ApiFetcher) initializer with the config
        super().__init__(config)

    def get_fixtures(
        self,
        date: Optional[str] = None,
        league: Optional[str] = None,
        season: Optional[str] = None,
        status: Optional[str] = None,
        **kwargs: dict[str, Union[int, str]],
    ) -> Response | None:
        return self.fetch_data(FIXTURES_API_ENDPOINT, **kwargs)

    ##### UPDATES METHODS #####
    def pull_fixtures_by_dates(self, dates_to_pull: list[str] | None) -> None:
        """
        Pull updated fixtures data from an external API and write it to JSON files.

        This function retrieves fixture data for specific dates and statuses from an external API
        and writes the response to JSON files. The function waits for a short duration between
        each API call to avoid exceeding rate limits.

        Returns:
            None
        """
        if not dates_to_pull:
            logging.info("No dates to update.")
            return None

        for single_date in dates_to_pull:
            endpoint = f"{FIXTURES_API_ENDPOINT}?date={single_date}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(
                resp, f"{FIXTURES_FILES_PREFIX}{single_date}", f"{FIXTURES_DIR}"
            )
            sleep(SLEEP_TIME)

    def pull_upcoming_fixtures_with_referees(self) -> None:
        """
        Pull upcoming fixtures data from an external API which has referee data and update Fixture table.

        This function retrieves fixture data for specific dates and statuses from an external API
        and writes the response to JSON files. The function waits for a short duration between
        each API call to avoid exceeding rate limits.

        Returns:
            None
        """
        date_to_pull = dt.datetime.today().strftime("%Y-%m-%d")
        endpoint = f"{FIXTURES_API_ENDPOINT}?date={date_to_pull}&status=NS"
        timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        resp = self.fetch_data(endpoint)
        self.write_response_to_json(
            resp, f"NOT_STARTED_{date_to_pull}_{timestamp}", f"{FIXTURES_DIR}"
        )
        return None

    def pull_updated_fixtures(self) -> None:
        # Method to pull updated fixtures in World/European Leagues or national Cups
        pass

    def pull_single_league_fixtures_for_all_seasons(
        self, league_id_to_pull: int
    ) -> None:
        """
        Pull fixtures data for all seasons of a single league from an external API and write it to JSON files.

        Args:
            league_id_to_pull (int): The ID of the league to pull fixtures for.

        Returns:
            None
        """
        with db.get_session() as session:
            try:
                leagues = (
                    session.query(League.league_id, League.league_name)
                    .filter(League.league_id == league_id_to_pull)
                    .first()
                )
                if leagues:
                    league_id, league_name = leagues
                season_years = (
                    session.query(Season.year)
                    .filter(Season.league_id == league_id_to_pull)
                    .all()
                )
                for season_year in season_years:
                    logging.info("Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        logging.info(
                            f"Pulling fixtures for {league_name}, season: {season_year[0]}..."
                        )
                        season_data = self.fetch_data(
                            f"{FIXTURES_API_ENDPOINT}?league={league_id}&season={season_year[0]}"
                        )
                        self.write_response_to_json(
                            season_data,
                            f"{league_id}-{league_name}-{season_year[0]}",
                            "fixtures/league_seasons",
                        )
                    except Exception as e:
                        logging.error(e)
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception

    def pull_single_league_fixtures_for_single_season(
        self, league_id_to_pull: int, season_id_to_pull: int
    ) -> None:
        """
        Pull fixtures data for a single league and season from an external API and write it to a JSON file.

        Args:
            league_id_to_pull (int): The ID of the league to pull fixtures for.
            season_id_to_pull (int): The ID of the season to pull fixtures for.

        Returns:
            None
        """
        with db.get_session() as session:
            try:
                leagues = (
                    session.query(League.league_id, League.league_name)
                    .filter(League.league_id == league_id_to_pull)
                    .first()
                )
                if leagues:
                    league_id, league_name = leagues
                logging.info(
                    f"Pulling fixtures for {league_name}, season: {season_id_to_pull}..."
                )
                season_data = self.fetch_data(
                    f"fixtures?league={league_id}&season={season_id_to_pull}"
                )
                self.write_response_to_json(
                    season_data,
                    f"{league_id}-{league_name}-{season_id_to_pull}",
                    f"{FIXTURES_DIR}/league_seasons",
                )
            except Exception as e:
                logging.error(e)
