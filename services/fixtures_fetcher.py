import logging
from dataclasses import dataclass
import datetime as dt
from time import sleep
from typing import Optional

from config.api_config import SLEEP_TIME, ApiConfig
from models.data.main import League, Season
from services.api_fetcher import APIFetcher
from services.db import Db

db = Db()


@dataclass
class FixtureParams:
    status: Optional[str] = None
    page: Optional[int] = None
    limit: Optional[int] = None


class FixtureFetcher(APIFetcher):
    def __init__(self, config: ApiConfig) -> None:  # noqa: F821
        # Call the parent class (ApiFetcher) initializer with the config
        super().__init__(config)

    def get_fixtures(
        self,
        date: Optional[str] = None,
        league: Optional[str] = None,
        season: Optional[str] = None,
        status: Optional[str] = None,
        **kwargs,
    ):
        return self.fetch_data("fixtures", **kwargs)

    ##### UPDATES METHODS #####
    def pull_finished_fixtures(self) -> None:
        """
        Pull updated fixtures data from an external API and write it to JSON files.

        This function retrieves fixture data for specific dates and statuses from an external API
        and writes the response to JSON files. The function waits for a short duration between
        each API call to avoid exceeding rate limits.

        Returns:
            None
        """
        # dates_to_pull = Fixture.get_fixtures_dates_to_be_updated()
        dates_to_pull = {
            "2024-10-06",
            "2024-10-05",
            "2024-09-29",
            "2024-09-28",
            "2024-09-24",
            "2024-09-21",
            "2024-08-31",
            "2024-08-25",
            "2024-08-24",
            "2024-08-15",
            "2024-08-07",
            "2024-06-10",
            "2024-06-08",
            "2024-05-19",
            "2024-05-04",
            "2024-05-01",
            "2024-04-25",
            "2024-04-20",
            "2024-04-19",
            "2024-04-17",
            "2024-04-16",
            "2024-04-14",
            "2024-04-13",
            "2024-04-12",
            "2024-04-07",
            "2024-04-06",
            "2024-04-05",
            "2024-04-03",
            "2024-04-02",
            "2024-04-01",
            "2024-03-28",
            "2024-03-27",
            "2024-03-23",
            "2024-03-22",
            "2024-03-21",
            "2024-03-17",
            "2024-03-16",
            "2024-03-15",
            "2024-03-13",
            "2024-03-12",
            "2024-03-10",
            "2024-03-09",
            "2024-03-08",
            "2024-03-07",
            "2024-03-06",
            "2024-03-05",
            "2024-03-03",
            "2024-03-02",
            "2024-03-01",
            "2024-02-27",
            "2024-02-26",
            "2024-02-25",
            "2024-02-24",
            "2024-02-23",
            "2024-02-22",
            "2024-02-21",
            "2024-02-13",
            "2024-02-12",
            "2024-02-10",
            "2024-02-03",
            "2024-01-31",
            "2024-01-28",
            "2024-01-27",
            "2024-01-24",
            "2024-01-23",
            "2024-01-18",
            "2024-01-17",
            "2024-01-16",
            "2024-01-14",
            "2024-01-13",
            "2024-01-12",
        }
        finished_statuses = "FT-AET-PEN-WO-ABD-CANC-AWD-WO-PST"

        if not dates_to_pull:
            logging.info("No dates to update.")

        for single_date in dates_to_pull:
            endpoint = f"fixtures?date={single_date}&status={finished_statuses}"
            timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(
                resp, f"FINISHED_{single_date}_{timestamp}", "fixtures/updates"
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
        endpoint = f"fixtures?date={date_to_pull}&status=NS"
        timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        resp = self.fetch_data(endpoint)
        self.write_response_to_json(
            resp, f"NOT_STARTED_{date_to_pull}_{timestamp}", "fixtures/updates"
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
                league_id, league_name = (
                    session.query(League.league_id, League.name)
                    .filter(League.league_id == league_id_to_pull)
                    .first()
                )
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
                            f"fixtures?league={league_id}&season={season_year[0]}"
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
                league_id, league_name = (
                    session.query(League.league_id, League.name)
                    .filter(League.league_id == league_id_to_pull)
                    .first()
                )
                logging.info(
                    f"Pulling fixtures for {league_name}, season: {season_id_to_pull}..."
                )
                season_data = self.fetch_data(
                    f"fixtures?league={league_id}&season={season_id_to_pull}"
                )
                self.write_response_to_json(
                    season_data,
                    f"{league_id}-{league_name}-{season_id_to_pull}",
                    "fixtures/league_seasons",
                )
            except Exception as e:
                logging.error(e)
