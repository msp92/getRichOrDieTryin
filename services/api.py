import json
import logging
from datetime import datetime
from time import sleep
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from config.api_config import (
    CURRENT_API,
    API_FOOTBALL_BASE_URL,
    API_FOOTBALL_HEADER_KEY_NAME,
    API_FOOTBALL_HEADER_KEY_VALUE,
    API_FOOTBALL_HEADER_HOST_NAME,
    API_FOOTBALL_HEADER_HOST_VALUE,
    RAPID_API_BASE_URL,
    RAPID_API_HEADER_KEY_NAME,
    RAPID_API_HEADER_KEY_VALUE,
    RAPID_API_HEADER_HOST_NAME,
    RAPID_API_HEADER_HOST_VALUE, SOURCE_DIR,
)
import requests
from models.data.main.countries import Country
from models.data.fixtures.fixtures import Fixture
from models.data.main.leagues import League
from models.data.main.seasons import Season
from services.db import Db

SLEEP_TIME = 5/60

db = Db()


class Api:
    def __init__(self, api_config: dict):
        self.url = api_config.get('url')
        self.key_name = api_config.get('key_name')
        self.key_value = api_config.get('key_value')
        self.host_name = api_config.get('host_name')
        self.host_value = api_config.get('host_value')

    def get_headers(self):
        return {
            self.key_name: self.key_value,
            self.host_name: self.host_value,
        }

    def check_subscription_status(self):
        url = f"{self.url}/status"
        response = requests.get(url, headers=self.get_headers())
        response_json = response.json()["response"]
        if response_json:
            logging.info(
                f"Hi {response_json['account']['firstname']}! "
                f"Your {response_json['subscription']['plan']} plan is active until {response_json['subscription']['end']}\n"
                f"Current usage: {response_json['requests']['current']}/{response_json['requests']['limit_day']}\n"
            )
            current_pull_limit = response_json['requests']['limit_day'] - response_json['requests']['current']
            return current_pull_limit
        logging.error("[ERROR] You have reached the request limit for today. Try again later.")
        return 0


    def get_json_from_api(self, endpoint: str):
        url = f"{self.url}/{endpoint}"
        response = requests.get(url, headers=self.get_headers())
        logging.info(f"Getting data from: {url}")
        if response.status_code == 200:
            if len(response.json()["response"]) == 0:
                logging.info(f"Response empty. No data have been pulled.")
                return None
            if response.json()["paging"]["total"] > 1:
                # FIXME: handle more than 1 page
                logging.warning(f"[FIXME] There are more than 1 page. Please handle this asap.")
                return None
            return response
        else:
            logging.error(f"Error fetching data from API. Status Code: {response.status_code}")
            return None


    def pull_single_league_fixtures_for_all_seasons(self, league_id_to_pull: int, pull_limit: int) -> None:
        """
            Pull fixtures data for all seasons of a single league from an external API and write it to JSON files.

            Args:
                league_id_to_pull (int): The ID of the league to pull fixtures for.
                pull_limit (int): The maximum number of seasons to pull fixtures for.

            Returns:
                None
        """
        with db.get_session() as session:
            try:
                league_id, league_name = session.query(League.league_id, League.name).filter(
                    League.league_id == league_id_to_pull
                ).first()
                season_years = (
                    session.query(Season.year).filter(Season.league_id == league_id_to_pull).all()
                )
                count = 0
                for season_year in season_years:
                    if count < pull_limit:
                        logging.info(f"Sleeping for a few seconds to avoid reaching limit.")
                        sleep(SLEEP_TIME)
                        try:
                            logging.info(
                                f"Pulling fixtures for {league_name}, season: {season_year[0]}..."
                            )
                            season_data = self.get_json_from_api(
                                f"fixtures?league={league_id}&season={season_year[0]}"
                            )
                            self.write_response_to_json(
                                season_data, f"{league_id}-{league_name}-{season_year[0]}", "fixtures/league_seasons"
                            )
                        except Exception as e:
                            logging.error(e)
                    count += 1
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception


    def pull_single_league_fixtures_for_single_season(self, league_id_to_pull: int, season_id_to_pull: int) -> None:
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
                league_id, league_name = session.query(League.league_id, League.name).filter(
                    League.league_id == league_id_to_pull
                ).first()
                logging.info(f"Pulling fixtures for {league_name}, season: {season_id_to_pull}...")
                season_data = self.get_json_from_api(
                    f"fixtures?league={league_id}&season={season_id_to_pull}"
                )
                self.write_response_to_json(
                    season_data, f"{league_id}-{league_name}-{season_id_to_pull}", "fixtures/league_seasons"
                )
            except Exception as e:
                logging.error(e)


    def pull_teams_for_countries_list(self, country_ids_list_to_pull: list, pull_limit: int):
        with db.get_session() as session:
            count = 0
            for country_id in country_ids_list_to_pull:
                country_name = session.query(Country.name).filter(
                    Country.country_id == country_id
                ).scalar()

                if count >= pull_limit:
                    break

                logging.info(f"Sleeping for a few seconds to avoid reaching limit.")
                sleep(SLEEP_TIME)

                try:
                    logging.info(f"Pulling teams for {country_name}...")
                    teams_data = self.get_json_from_api(f"teams?country={country_name}")
                    # Check if teams_data is not empty
                    if not teams_data:
                        logging.info("No data found for", country_name)
                        continue
                    self.write_response_to_json(teams_data, f"{country_id}_{country_name}", "teams")
                    count += 1
                except Exception as e:
                    logging.error(e)
                    continue


    def pull_statistics_fixtures_for_leagues_and_seasons(
        self, league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
    ):
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name).filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"])
                        )
                    ).all()
                )
                count = 0
                for fixture in fixtures:
                    if count < pull_limit:
                        logging.info(f"Sleeping for a few seconds to avoid reaching limit.")
                        sleep(SLEEP_TIME)
                        try:
                            logging.info(
                                f"Pulling statistics fixtures for fixture: {fixture.fixture_id} "
                                f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                            )
                            statistics_fixtures_data = self.get_json_from_api(
                                f"fixtures/statistics?fixture={fixture.fixture_id}"
                            )

                        except Exception as e:
                            logging.error(e)
                            continue
                    self.write_response_to_json(
                        statistics_fixtures_data, f"{fixture.fixture_id}_statistics_fixtures", "statistics_fixtures"
                    )
                    count += 1
            except Exception as e:
                raise Exception


    def pull_player_statistics_for_leagues_and_seasons(
        self, league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
    ):
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name).filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"])
                        )
                    ).all()
                )
                count = 0
                for fixture in fixtures:
                    if count < pull_limit:
                        logging.info(f"Sleeping for a few seconds to avoid reaching limit.")
                        sleep(SLEEP_TIME)
                        try:
                            logging.info(
                                f"Pulling player statistics for fixture: {fixture.fixture_id} "
                                f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                            )
                            statistics_fixtures_data = self.get_json_from_api(
                                f"fixtures/players?fixture={fixture.fixture_id}"
                            )
                        except Exception as e:
                            logging.error(e)
                            continue
                    self.write_response_to_json(
                        statistics_fixtures_data, f"{fixture.fixture_id}_player_statistics", "player_statistics"
                    )
                    count += 1
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception


    def pull_events_for_leagues_and_seasons(
        self, league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
    ):
        finished_statuses = ["FT", "AET", "PEN", "WO"]
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name).filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(finished_statuses)
                        )
                    ).all()
                )
                count = 0
                for fixture in fixtures:
                    if count < pull_limit:
                        logging.info(f"Sleeping for a few seconds to avoid reaching limit.")
                        sleep(SLEEP_TIME)
                        try:
                            logging.info(
                                f"Pulling player statistics for fixture: {fixture.fixture_id} "
                                f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                            )
                            events_fixtures_data = self.get_json_from_api(
                                f"fixtures/events?fixture={fixture.fixture_id}"
                            )
                            self.write_response_to_json(
                                events_fixtures_data, f"{fixture.fixture_id}_events", "events"
                            )
                        except Exception as e:
                            logging.error(e)
                    count += 1
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception


    def write_response_to_json(self, response, filename, subdir=""):
        if response:
            # file_path = os.path.join(SOURCE_DIR, subdir, f"{filename}.json")
            file_path = f"../{SOURCE_DIR}/{subdir}/{filename}.json"
            try:
                with open(file_path, "w") as file:
                    json.dump(response.json(), file)
                return True
            except (AttributeError, IOError) as e:
                logging.error(f"Writing response to '{file_path}' has failed: {e}")
        return False

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
        dates_to_pull = Fixture.get_fixtures_dates_to_be_updated()
        finished_statuses = "FT-AET-PEN-WO"

        if not dates_to_pull:
            logging.info(f"No dates to update.")

        for single_date in dates_to_pull:
            endpoint = f"fixtures?date={single_date}&status={finished_statuses}"
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            resp = self.get_json_from_api(endpoint)
            self.write_response_to_json(resp, f"FINISHED_{single_date}_{timestamp}", "fixtures/updates")
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
        date_to_pull = datetime.today().strftime('%Y-%m-%d')
        endpoint = f"fixtures?date={date_to_pull}&status=NS"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        resp = self.get_json_from_api(endpoint)
        self.write_response_to_json(resp, f"NOT_STARTED_{date_to_pull}_{timestamp}", "fixtures/updates")
        return None


    def pull_updated_fixtures(self) -> None:
        # Method to pull updated fixtures in World/European Leagues or national Cups
        pass
